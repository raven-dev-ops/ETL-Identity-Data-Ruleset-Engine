# Customer Handoff Package

This document packages the repo-side customer handoff baseline for the
supported protected-pilot and production-like operating model.

Use it after the protected-pilot promotion is sealed and the production
acceptance suite reports `ready` or `ready_with_advisories`.

It does not replace agency incident command, legal review, personnel
screening, or other governance that remains outside the product runtime.

## Required Handoff Packet

Deliver these artifacts together:

- the sealed `protected_pilot_promotion_manifest.json`
- the latest `production_acceptance_report.json` and
  `production_acceptance_report.md`
- the latest `cjis_evidence_review_index.json` and
  `cjis_evidence_review_index.md`
- the latest referenced CJIS evidence pack zip
- the rollback bundle path recorded in the promotion manifest
- the runtime snapshot and HA rehearsal summary recorded in the
  promotion manifest
- this handoff package plus the linked operational docs

The operator should be able to answer these questions from the packet
without opening source code:

- what build and config were promoted
- which evidence pack and cadence record are current
- which rollback artifact is trusted
- which commands are supported for response, review, and escalation
- which responsibilities remain outside the repo

## Incident Response Runbook

Use this repo-side flow for service failure, suspicious operator
activity, replay or publish mistakes, or evidence that the promoted
runtime has drifted away from the sealed packet.

### Trigger Conditions

Start the incident flow when any of these occur:

- `/healthz` or `/readyz` fail unexpectedly
- a protected-pilot promotion prerequisite is no longer true
- recent audit events show an unexpected replay, publish, export, or
  review action
- a credential, token, or support artifact may have been exposed
- tenant isolation, field masking, or rollback behavior is in doubt

### Immediate Containment

1. Stop new privileged actions.
2. Remove external exposure from the service, or stop the Windows
   service wrappers when operating the packaged single-host pilot.
3. Preserve the promoted runtime state, recent logs, replay bundles,
   and rollback bundle. Do not delete the evidence you will need for
   recovery.
4. If secrets may be exposed, follow [SECURITY.md](../SECURITY.md)
   immediately and rotate the affected material outside the repo.

### Required Evidence Collection

Collect these repo-side artifacts before recovery:

- current `production_acceptance_report.json`
- current `protected_pilot_promotion_manifest.json`
- current `cjis_evidence_review_index.json`
- the latest referenced CJIS evidence pack zip
- recent audit events from `GET /api/v1/audit-events` or the admin
  console
- current `/healthz`, `/readyz`, and `/api/v1/metrics` output when the
  service is still reachable

If you are operating the packaged Windows single-host pilot, also run:

```powershell
.\launch\collect_support_bundle.ps1
```

That bundle is the preferred escalation artifact for the single-host
pilot because it already redacts the runtime env, logs, and persisted
audit details into one handoff zip.

### Recovery Decision

Use these boundaries:

- service or deployment failure:
  use [production-operating-model.md](production-operating-model.md)
  plus the HA and recovery docs to restore the supported runtime
- persisted-state corruption or bad operator action:
  use the rollback bundle and replay recovery path recorded in the
  promotion manifest
- credential or data-exposure incident:
  follow [SECURITY.md](../SECURITY.md) first, then rebuild trust in the
  runtime before reopening traffic
- single-host pilot troubleshooting:
  capture the support bundle before reseed or teardown

After recovery, rerun the evidence capture cadence and the production
acceptance suite so the post-incident state is documented from the same
repo-side baseline as the original promotion.

## Audit Review SOP

This repo supports audit review through three aligned surfaces:

- `GET /api/v1/audit-events`
- the admin console recent-audit view
- the CJIS evidence pack, which carries recent audit events forward into
  the review cadence

### Minimum Review Triggers

Perform an audit review:

- on each scheduled CJIS evidence cadence review
- after every replay, publish, export, or rollback event
- after any auth, tenant-boundary, or masking-policy change
- during incident response when privileged activity may be part of the
  cause

### Review Procedure

1. Confirm the reviewer identity and timestamp in the local operating
   record outside the repo.
2. Pull the recent persisted audit events from the service or evidence
   pack.
3. Verify that privileged actions map to an approved change window and
   to the expected tenant.
4. Confirm that replay, publish, export, and review actions match the
   sealed promotion packet or the approved operating change.
5. Escalate any unexplained privileged activity before the next cutover
   or cadence signoff.
6. Record the review outcome through the CJIS cadence tooling:

```powershell
.\.venv\Scripts\python.exe scripts/manage_cjis_evidence_cadence.py review `
  --output-dir dist\cjis-evidence-review `
  --capture-id <capture-id> `
  --reviewer "<reviewer-name>"
```

The repo does not replace the customer or agency ticketing system. The
review record in the cadence index is the repo-side evidence pointer,
not the full organizational disposition record.

## Support-Bundle Collection Guide

The support-bundle workflow is currently supported for the packaged
Windows single-host customer pilot. It is not the primary artifact for
the protected-pilot Kubernetes or external-HA PostgreSQL baseline.

### When To Use It

Generate the support bundle:

- before reseed or teardown of a problematic pilot install
- before patch upgrade when behavior differs from the shipped walkthrough
- when maintainers need redacted runtime evidence from the packaged
  pilot

### Command

From the extracted customer pilot bundle root:

```powershell
.\launch\collect_support_bundle.ps1
```

### Contents

The bundle includes:

- pilot and handoff manifests
- redacted runtime config and env material
- redacted local logs
- persisted-state metadata
- recent runs and audit events
- Windows service status when available

### Boundary

Do not treat the support bundle as a full production-forensics capture.
For the protected-pilot runtime, pair deployment-native logs, backup
artifacts, and network or IdP evidence with:

- the sealed promotion manifest
- the latest CJIS evidence pack
- the latest production acceptance report

## Operator Training Walkthrough

Run this walkthrough before handoff signoff. Use synthetic or masked
acceptance data only.

### Day-0 Topics

1. Review the sealed promotion manifest and identify the promoted build,
   runtime snapshot, evidence pack, and rollback bundle.
2. Review the latest production acceptance report and distinguish
   blocking findings from advisories.
3. Review the current CJIS evidence cadence record and confirm the due
   date and latest reviewer.

### Day-1 Operator Drill

1. Start from the supported runtime for the deployment.
2. Verify `/healthz`, `/readyz`, and `/api/v1/metrics`.
3. Use the admin console or service API to inspect recent audit events.
4. Confirm the operator can locate the current custody manifest,
   acceptance summary, and rollback bundle referenced from the promotion
   packet.
5. Run one evidence-cadence `status` check:

```powershell
.\.venv\Scripts\python.exe scripts/manage_cjis_evidence_cadence.py status `
  --output-dir dist\cjis-evidence-review
```

6. Walk the incident-response flow and identify which artifact would be
   collected first for the current deployment topology.

### Optional Single-Host Pilot Drill

If the customer is also receiving the packaged Windows pilot, add:

1. run `.\launch\check_pilot_readiness.ps1`
2. start the demo shell or service
3. generate one redacted support bundle
4. review the pilot rollback or reseed commands in
   [customer-pilot-runbooks.md](customer-pilot-runbooks.md)

## Responsibility Matrix

| Surface | Repo maintainers | Customer platform operators | Customer data operators | Agency / governance owners |
| --- | --- | --- | --- | --- |
| Runtime and service behavior | Maintain documented CLI, API, config, and migration behavior | Deploy the runtime, manage ingress, secrets, TLS, storage, and backups | Use supported run, review, replay, publish, and export flows | Approve operational use inside agency policy |
| Protected-pilot promotion and rollback packet | Provide the promotion, acceptance, and rollback tooling | Preserve the sealed promotion packet and execute rollback when required | Validate the promoted data workflow before cutover | Approve change windows and release governance |
| Audit review and CJIS evidence cadence | Provide the evidence-pack and cadence tooling | Run the cadence, retain evidence, and surface deployment-side logs | Review replay, publish, export, and review activity for legitimacy | Define review obligations, signoff rules, and retention policy |
| Support-bundle workflow | Provide the redacted single-host pilot bundle collector | Run it when operating the packaged Windows pilot | Attach it to operator escalation when the issue is data- or workflow-related | Control where the artifact may be stored or shared |
| Incident response and security events | Document repo-side boundaries and reporting path | Contain service exposure, rotate secrets, restore systems, and preserve evidence | Pause risky operator actions and validate recovered state | Own incident command, legal notice, personnel response, and external reporting |
| Identity data governance | Keep the public repo synthetic-only and document supported product boundaries | Keep production data, backups, logs, and manifests out of source control | Avoid exporting or sharing raw operational identifiers outside approved channels | Own CJIS, privacy, records, and contractual obligations |

## Outside-Repo Responsibilities

The repo does not ship or operate:

- a ticketing or incident-command system
- a SIEM, IdP, firewall, or endpoint-management platform
- agency policy acceptance, legal review, or contract execution
- personnel screening, MFA enforcement, or physical-facility controls
- the customer's retention schedule for operational evidence

Those responsibilities remain with the customer or agency even when the
repo provides a supporting runtime artifact or checklist.
