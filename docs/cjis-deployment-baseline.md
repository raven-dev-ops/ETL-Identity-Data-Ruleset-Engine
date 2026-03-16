# CJIS Deployment Baseline

This document defines the repo-side baseline for deploying the runtime
in a CJIS-sensitive environment.

It is a deployment checklist and preflight, not a compliance
certification.

## Goal

Make the runtime fail fast when a deployment is still only "demo ready"
instead of meeting the minimum repo-side baseline for a CJIS-aligned
production rollout.

## Runtime Profile

Use the dedicated `cjis` runtime environment in
`config/runtime_environments.yml`.

That profile requires:

- PostgreSQL state storage
- JWT bearer auth
- RS256 token validation
- deployment-supplied object-storage credentials

## Required Environment Surface

Start from:

- `deploy/cjis.env.example`

The shipped preflight expects these categories to be configured:

- state store: `ETL_IDENTITY_STATE_DB`
- object storage: access and secret key, preferably through
  `ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY_FILE` and
  `ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY_FILE`
- JWT auth: issuer, audience, and a PEM public key, preferably through
  `ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM_FILE`
- TLS material: certificate and key paths
- audit path: `ETL_IDENTITY_AUDIT_LOG_DIR`
- backup path: `ETL_IDENTITY_BACKUP_ROOT`
- optional runtime auth rotation gate:
  - `ETL_IDENTITY_RUNTIME_AUTH_MAX_AGE_HOURS`
- deployment attestations:
  - `ETL_IDENTITY_CJIS_ENCRYPTION_AT_REST`
  - `ETL_IDENTITY_CJIS_MFA_ENFORCED`
  - `ETL_IDENTITY_CJIS_PERSONNEL_SCREENING`
  - `ETL_IDENTITY_CJIS_SECURITY_ADDENDUM`
  - `ETL_IDENTITY_CJIS_AUDIT_REVIEW`
- incident contact:
  - `ETL_IDENTITY_CJIS_INCIDENT_CONTACT`

## Preflight

Run the repo-side preflight before deployment signoff:

```bash
python scripts/cjis_preflight_check.py --environment cjis --runtime-config config/runtime_environments.yml
```

For a service-only startup gate over the same runtime profile:

```bash
python -m etl_identity_engine.cli check-runtime-auth-material \
  --environment cjis \
  --runtime-config config/runtime_environments.yml \
  --max-secret-file-age-hours 720
```

What it enforces:

- the selected runtime environment loads cleanly
- state storage resolves to PostgreSQL, not SQLite
- service auth is JWT on the `Authorization` header
- JWT validation is pinned to `RS256`
- mounted secret-file inputs resolve cleanly when `_FILE` variables are
  used
- file-backed auth material can be age-checked for rotation health
- object-storage secrets are present
- required TLS, audit, and backup paths exist
- required deployment attestation flags are affirmative

The runtime observability baseline also now redacts raw auth material
and free-text review-note content from structured logs and persisted
audit-event details. That reduces the chance that operational traces
become an uncontrolled copy of sensitive request content.

The script emits JSON and exits non-zero on failure.

## Evidence Pack

To package the current repo-side CJIS review evidence into one zip:

```bash
python scripts/package_cjis_evidence_pack.py --environment cjis --runtime-config config/runtime_environments.yml --output-dir dist/cjis-evidence
```

If your deployment material is stored in a local `KEY=VALUE` file, add:

```bash
python scripts/package_cjis_evidence_pack.py --environment cjis --runtime-config config/runtime_environments.yml --output-dir dist/cjis-evidence --env-file deploy/cjis.env
```

The pack includes:

- the current CJIS preflight summary
- a redacted runtime-environment summary
- state-store backend and schema summary
- operational metrics and recent audit events
- an optional selected persisted run summary
- `standards_mapping_index.json`
- reference copies of the standards mapping, baseline doc, env template,
  and runtime catalog

The evidence pack supports review conversations and deployment
verification. It does not, by itself, claim full operational CJIS
compliance.

For protected-pilot cutover, seal the evidence pack together with the
runtime snapshot, custody manifest, acceptance summary, HA rehearsal
summary, and rollback bundle by following
[protected-pilot-promotion.md](protected-pilot-promotion.md).

For steady-state operation after cutover, run the recurring repo-side
evidence loop in [cjis-evidence-cadence.md](cjis-evidence-cadence.md)
so the latest evidence capture has an explicit due date and overdue
status.

## Recommended Rollout Order

1. Validate the runtime with synthetic data and the standalone demo.
2. Deploy the service and batch runtime in a protected environment.
3. Run `state-db-upgrade` against the target PostgreSQL store.
4. Run the CJIS preflight with deployment variables populated.
5. Run the persisted-state recovery smoke path against the protected
   deployment topology.
6. Seal the protected-pilot promotion inputs with
   [protected-pilot-promotion.md](protected-pilot-promotion.md).
7. Only then move into agency-specific acceptance and governance work.

## External References

- CJIS Security Policy v5.9.5:
  <https://le.fbi.gov/file-repository/cjis-security-policy-v5_9_5-20240709.pdf>
- CJIS Security Addendum:
  <https://le.fbi.gov/cjis-division/cjis-security-policy-resource-center/appendicies/security-addendum.pdf/view>

## Scope Boundary

This baseline helps with:

- deployment consistency
- auth and storage posture
- audit/logging readiness
- backup/recovery readiness

It does not, by itself, satisfy:

- policy acceptance by a CSA or agency
- personnel vetting requirements
- contract and addendum execution
- physical and administrative controls outside the product runtime
