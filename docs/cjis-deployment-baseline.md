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
- object storage: access and secret key
- JWT auth: issuer, audience, public key
- TLS material: certificate and key paths
- audit path: `ETL_IDENTITY_AUDIT_LOG_DIR`
- backup path: `ETL_IDENTITY_BACKUP_ROOT`
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

What it enforces:

- the selected runtime environment loads cleanly
- state storage resolves to PostgreSQL, not SQLite
- service auth is JWT on the `Authorization` header
- JWT validation is pinned to `RS256`
- object-storage secrets are present
- required TLS, audit, and backup paths exist
- required deployment attestation flags are affirmative

The script emits JSON and exits non-zero on failure.

## Recommended Rollout Order

1. Validate the runtime with synthetic data and the standalone demo.
2. Deploy the service and batch runtime in a protected environment.
3. Run `state-db-upgrade` against the target PostgreSQL store.
4. Run the CJIS preflight with deployment variables populated.
5. Run the persisted-state recovery smoke path against the protected
   deployment topology.
6. Only then move into agency-specific acceptance and governance work.

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
