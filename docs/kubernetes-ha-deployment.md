# Kubernetes HA Deployment

The repo now ships an external-HA PostgreSQL reference baseline for the
application layer.

This baseline is intentionally narrow:

- the PostgreSQL cluster itself is managed outside the repo
- the runtime connects through one stable writer endpoint
- the service runs as a two-replica Deployment with pod anti-affinity
- the app tier keeps the same one-off migration and manifest-batch job
  model as the single-node cluster baseline

Use this baseline when your platform already provides an operator- or
cloud-managed PostgreSQL HA service and you want the repo-supported app
manifests, failover checks, and restore/replay runbook around it.

## Shipped Assets

- `deploy/kubernetes-ha/kustomization.yaml`
- `deploy/kubernetes-ha/service-deployment.yaml`
- `deploy/kubernetes-ha/pod-disruption-budget.yaml`
- `deploy/kubernetes-ha/runtime-secret.example.yaml`
- `deploy/kubernetes-ha/state-db-upgrade-job.yaml`
- `deploy/kubernetes-ha/batch-job.yaml`
- `deploy/kubernetes-ha/external-writer-service.example.yaml`

The baseline reuses these common assets from `deploy/kubernetes/`:

- `namespace.yaml`
- `landing-pvc.yaml`
- `service-service.yaml`
- `service-ingress.example.yaml`

## Runtime Environment

The HA baseline uses the dedicated `cluster_ha` runtime environment from
`config/runtime_environments.yml`.

That environment is intentionally configured for:

- PostgreSQL via `ETL_IDENTITY_STATE_DB`
- API-key service auth compatibility mode
- the same stable scope contract used by the rest of the runtime

The shipped app layer therefore stays self-contained for probes and
smoke tests. Teams that need JWT-backed ingress can overlay the same
manifests onto the `prod` runtime environment after validating the HA
baseline first.

## Writer Endpoint Contract

The supported contract is one stable writer endpoint for the service,
batch job, and migration job.

The shipped example uses:

- host: `identity-postgres-rw`
- port: `5432`
- connection option: `target_session_attrs=read-write`

The repo does not ship PostgreSQL replication, consensus, or promotion
logic. The platform must provide those through:

- a managed PostgreSQL writer DNS name
- an operator-managed in-cluster writer Service
- or an `ExternalName` Service derived from
  `deploy/kubernetes-ha/external-writer-service.example.yaml`

## Apply Order

### 1. Provision the external PostgreSQL HA writer endpoint

Before applying the app manifests, confirm the platform exposes one
writer endpoint that always resolves to the promoted primary.

### 2. Prepare the runtime Secret and image tag

Start from `deploy/kubernetes-ha/runtime-secret.example.yaml` and
provide:

- `ETL_IDENTITY_STATE_DB`
- `ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY`
- `ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY`
- `ETL_IDENTITY_SERVICE_READER_API_KEY`
- `ETL_IDENTITY_SERVICE_OPERATOR_API_KEY`
- `ETL_IDENTITY_SERVICE_READER_TENANT_ID`
- `ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID`

The API-key compatibility baseline still binds each service principal to
one tenant. The shipped examples default those tenant IDs to `default`;
set them to the deployment tenant before applying the Secret.

Replace the image tag in:

- `deploy/kubernetes-ha/service-deployment.yaml`
- `deploy/kubernetes-ha/state-db-upgrade-job.yaml`
- `deploy/kubernetes-ha/batch-job.yaml`

### 3. Apply namespace, storage, and service tier

```bash
kubectl apply -f deploy/kubernetes-ha/runtime-secret.yaml
kubectl apply -k deploy/kubernetes-ha
```

### 4. Run the state migration job

```bash
kubectl apply -f deploy/kubernetes-ha/state-db-upgrade-job.yaml
kubectl wait --for=condition=complete job/identity-ha-state-db-upgrade -n etl-identity --timeout=300s
```

### 5. Load a landed batch and run the batch job

The batch job expects the same landing PVC and manifest contract as the
single-node Kubernetes baseline:

- `/runtime/landing/batch-manifest.yaml`
- the landed files referenced by that manifest

Run the job with:

```bash
kubectl apply -f deploy/kubernetes-ha/batch-job.yaml
kubectl wait --for=condition=complete job/identity-ha-batch -n etl-identity --timeout=1800s
```

## Failover Runbook

When the external PostgreSQL platform promotes a new writer:

1. Confirm the writer endpoint now resolves to the promoted primary.
2. Wait for Kubernetes to keep at least one service replica available;
   the shipped pod disruption budget preserves `minAvailable: 1`.
3. Check service readiness:

```bash
kubectl get pods -n etl-identity -l app.kubernetes.io/component=service-api
kubectl logs deploy/identity-service -n etl-identity --tail=100
curl -H "X-API-Key: <reader-key>" http://identity-service.etl-identity.svc.cluster.local:8000/readyz
```

The runtime configures PostgreSQL connections with pool pre-ping so a
worker can discard stale pooled connections after writer replacement and
reconnect through the same writer endpoint.

## Backup, Restore, And Rollback

Use the repo-native encrypted backup workflow against the HA writer
endpoint:

```bash
python -m etl_identity_engine.cli backup-state-bundle \
  --state-db "postgresql://etl_identity:<password>@identity-postgres-rw:5432/identity_state?target_session_attrs=read-write" \
  --output recovery/pipeline_state_backup_encrypted.zip \
  --include-path /runtime/output/data/replay_bundles/RUN-20260314T000000Z-ABC12345 \
  --passphrase-file /secrets/state-backup-passphrase.txt
```

Restore into a clean PostgreSQL target before replay or rollback:

```bash
python -m etl_identity_engine.cli restore-state-bundle \
  --state-db "postgresql://etl_identity:<password>@identity-postgres-restore:5432/identity_state?target_session_attrs=read-write" \
  --bundle recovery/pipeline_state_backup_encrypted.zip \
  --attachments-output-dir /runtime/output/data/replay_bundles \
  --passphrase-file /secrets/state-backup-passphrase.txt
```

After restore:

- rebuild downstream outputs with `report` or `publish-run`
- validate review state with `review-case-list`
- execute `replay-run` when you need a fresh recovered run from the
  restored replay bundle

See [recovery-runbooks.md](recovery-runbooks.md) for the detailed
restore and replay procedure.

## Rehearsal Validation

The repo includes an executable HA rehearsal:

```bash
python scripts/postgresql_ha_rehearsal.py \
  --image-tag etl-identity-engine:ha-rehearsal \
  --service-port 18082 \
  --writer-port 55440 \
  --restore-port 55441
```

That rehearsal validates:

- the shipped HA manifests and secret contract
- schema upgrade against the writer endpoint
- a manifest-driven batch run against the writer endpoint
- service reconnect after simulated writer failover
- encrypted backup creation from PostgreSQL state plus the archived
  replay bundle
- restore into a clean PostgreSQL target
- report rebuild and replay from the restored state

## Current Boundary

This baseline provides:

- a repo-supported app-tier HA reference topology for Kubernetes
- one stable writer-endpoint contract for managed PostgreSQL HA
- a concrete failover, backup, restore, and replay runbook
- an executable rehearsal path that validates the documented commands

It does not provide:

- PostgreSQL replication or consensus manifests
- automated database promotion
- multi-region topology management
