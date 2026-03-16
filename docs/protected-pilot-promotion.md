# Protected Pilot Promotion

This workflow seals the repo-side inputs required to move from a
validated lower environment into a protected pilot deployment.

It is an operator control for promotion consistency and rollback
discipline. It is not, by itself, an agency approval or compliance
certification.

## Required Inputs

Before cutover, collect one immutable input set:

- customer pilot bundle with a passing handoff-manifest verification
- protected-pilot runtime environment catalog and a deployment-specific
  `KEY=VALUE` env snapshot
- landed-batch `custody_manifest.json`
- masked live-target `acceptance_package_summary.json`
- CJIS evidence-pack zip
- PostgreSQL HA rehearsal summary captured from
  `scripts/postgresql_ha_rehearsal.py`
- rollback bundle recorded before cutover, typically
  `backup-state-bundle` output

The promotion seal records hashes for those artifacts, copies the safe
JSON manifests needed for review, snapshots the runtime catalog, and
stores the rollback and revalidation instructions in one manifest.

## Seal Command

Capture the HA rehearsal JSON to a file first:

```powershell
.\.venv\Scripts\python.exe scripts/postgresql_ha_rehearsal.py --image-tag etl-identity-engine:ha-local --service-port 18082 --writer-port 55440 --restore-port 55441 > dist\protected-pilot-inputs\postgresql_ha_rehearsal.json
```

Then seal the promotion set:

```powershell
.\.venv\Scripts\python.exe scripts/seal_protected_pilot_promotion.py `
  --bundle dist\customer-pilot\etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions.zip `
  --runtime-config config\runtime_environments.yml `
  --environment cjis `
  --env-file deploy\protected-pilot.env `
  --custody-manifest dist\live-custody\20260315T120000Z-cad_county_dispatch_v1\custody_manifest.json `
  --acceptance-summary dist\live-acceptance\20260315T121500Z-cad_county_dispatch_v1-acceptance\acceptance_package_summary.json `
  --evidence-pack dist\cjis-evidence\etl-identity-engine-vX.Y.Z-cjis-evidence-cjis.zip `
  --ha-rehearsal-summary dist\protected-pilot-inputs\postgresql_ha_rehearsal.json `
  --rollback-bundle dist\protected-pilot-inputs\pipeline_state_backup_encrypted.zip `
  --output-dir dist\protected-pilot-promotions
```

The command writes a timestamped directory under
`dist/protected-pilot-promotions/` containing:

- `protected_pilot_promotion_manifest.json`
- `protected_pilot_promotion_summary.md`
- `runtime_env_fingerprint.json`
- `reference/runtime_environments.yml`
- copied review inputs:
  - `inputs/pilot_manifest.json`
  - `inputs/pilot_handoff_manifest.json`
  - `inputs/custody_manifest.json`
  - `inputs/acceptance_package_summary.json`
  - `inputs/postgresql_ha_rehearsal.json`
  - `inputs/cjis_evidence_manifest.json`

The env snapshot is hashed and inventoried, but not copied, because it
typically contains deployment secrets.

## What The Seal Enforces

The promotion seal fails closed if any of these are not true:

- the pilot handoff manifest verifies cleanly
- custody and acceptance artifacts point at the same live target
- the acceptance package confirms it was generated from a captured
  custody root
- the evidence pack is for the selected environment and reports
  `preflight_status: ok`
- the HA rehearsal reports the documented writer-failover and
  restore-to-replay steps
- the selected protected-pilot state store resolves to PostgreSQL
- the protected-pilot writer connection includes
  `target_session_attrs=read-write`
- the current state-store revision matches the repo migration head

## Rollback

The seal records the rollback bundle path and the minimum rollback
sequence:

1. Stop protected-pilot service and batch entrypoints.
2. Restore the recorded rollback bundle with `restore-state-bundle`.
3. Restore the replay-bundle attachments expected by the runtime.
4. Rerun readiness, preflight, and evidence-pack revalidation before
   reopening operator or consumer access.

The rollback bundle should be immutable and stored outside the repo
working tree used for ordinary development.

## Revalidation

Every seal records the repo-side revalidation commands for the promoted
input set:

- `scripts/check_customer_pilot_readiness.py`
- `scripts/cjis_preflight_check.py`
- `scripts/package_cjis_evidence_pack.py`

Run those again after cutover, after rollback, and after any config or
artifact change that would invalidate the sealed input set.

## Scope Boundary

This workflow helps with:

- promotion consistency
- immutable input review
- rollback traceability
- repo-side revalidation discipline

It does not, by itself, cover:

- agency signoff
- operator staffing or training obligations
- incident command outside the shipped runtime surfaces
- administrative or legal controls outside the product runtime
