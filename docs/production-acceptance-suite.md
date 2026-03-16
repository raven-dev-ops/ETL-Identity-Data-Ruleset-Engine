# Production Acceptance Suite

This suite turns the protected-pilot promotion packet and CJIS evidence
cadence into one cutover readiness artifact.

It emits:

- `production_acceptance_report.json`
- `production_acceptance_report.md`

The report is machine-readable and classifies findings as either
blocking or advisory.

## Inputs

The suite expects:

- a sealed `protected_pilot_promotion_manifest.json`
- a CJIS evidence review index from
  `scripts/manage_cjis_evidence_cadence.py`
- optional live service probe inputs:
  - `--service-base-url`
  - `--service-header Name=Value`

The suite reads the referenced custody manifest, acceptance summary,
evidence pack, HA rehearsal summary, rollback bundle path, and runtime
summary from the promotion packet.

## Command

```powershell
.\.venv\Scripts\python.exe scripts/production_acceptance_suite.py `
  --promotion-manifest dist\protected-pilot-promotions\20260315T123000Z-cjis-pilot-promotion\protected_pilot_promotion_manifest.json `
  --evidence-review-index dist\cjis-evidence-review\cjis_evidence_review_index.json `
  --output-dir dist\production-acceptance `
  --service-base-url https://pilot.example.gov `
  --service-header "Authorization=Bearer <reader-token>"
```

If you do not provide a live service URL, the suite still emits the
report, but it records the service probes as advisory skips instead of
blocking passes.

## Blocking Checks

These checks block readiness when they fail:

- promotion manifest is sealed
- all promotion-manifest prerequisite checks are `ok`
- tenant isolation is configured in the protected-pilot auth surface
- custody manifest is present and captured
- acceptance package is present and passes masked validation
- evidence pack reports `preflight_status: ok`
- the latest CJIS evidence cadence state is `current`
- the current cadence record matches the promoted evidence pack
- rollback bundle path exists
- HA rehearsal includes writer failover and restore-to-replay coverage
- protected-pilot state store is PostgreSQL and at migration head
- live `/healthz` and `/readyz` probes succeed when a service URL is
  provided

## Advisory Checks

These do not block readiness by themselves:

- live service probes were not run because no service URL or auth header
  was provided

The suite can be extended with more advisory checks later, but the
blocking rules above are the current repo-side cutover floor.

## Scope Boundary

This suite helps answer:

- is the protected-pilot packet internally consistent
- is the reviewed evidence current
- does the repo-side readiness packet meet the documented baseline

It does not, by itself, answer:

- whether the agency has completed policy signoff
- whether operator staffing and training are complete
- whether non-product operational controls are satisfied

Use [customer-handoff-package.md](customer-handoff-package.md) for the
repo-side incident-response, audit-review, support-bundle, and training
baseline that sits next to this readiness report.
