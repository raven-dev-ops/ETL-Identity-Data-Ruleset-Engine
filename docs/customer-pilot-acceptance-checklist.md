# Customer Pilot Acceptance Checklist

Use this checklist during customer handoff for the supported
Windows-first single-host pilot baseline.

## Bundle Integrity

- [ ] The delivered zip filename matches the agreed pilot build.
- [ ] `pilot_manifest.json` is present.
- [ ] `pilot_handoff_manifest.json` is present.
- [ ] The readiness check passes against the delivered bundle.

## Host Readiness

- [ ] Windows host confirmed.
- [ ] Python `3.11+` confirmed.
- [ ] Docker Desktop installed and running.
- [ ] The documented install root is writable.
- [ ] Free disk space meets the readiness-check requirement.

## Bootstrap

- [ ] `.\launch\check_pilot_readiness.ps1` completed successfully.
- [ ] `.\launch\bootstrap_windows_pilot.ps1 --prepare-only` completed successfully.
- [ ] `runtime/pilot_bootstrap.json` exists.
- [ ] `runtime/pilot_runtime.env` exists.
- [ ] The PostgreSQL pilot container is running.

## Demo Shell

- [ ] `.\launch\start_pilot_demo_shell.ps1` starts successfully.
- [ ] The overview page loads at the expected URL.
- [ ] The seeded scenarios are visible.
- [ ] A golden-person detail page opens correctly.

## Service API

- [ ] `.\launch\start_pilot_service.ps1` starts successfully.
- [ ] The operator has the documented reader and operator API keys.
- [ ] A persisted public-safety read-model lookup succeeds.

## Walkthrough Outcomes

- [ ] `CAD And RMS On One Identity` was demonstrated.
- [ ] A cross-system golden-person activity summary was demonstrated.
- [ ] The delivered bundle's synthetic-only scope was stated clearly.
- [ ] The CJIS boundary was stated clearly: this pilot is not a claim of
  full CJIS operational compliance.

## Handoff Signoff

- [ ] Customer operator received the runbooks.
- [ ] Customer operator received the readiness-check output.
- [ ] Customer operator received the original pilot zip.
- [ ] Customer operator received the rollback instructions.

## Signoff Record

- Customer / agency:
- Pilot bundle version:
- Delivery date:
- Operator name:
- Reviewer name:
- Acceptance result:
- Notes:
