# Customer Pilot Readiness

The customer pilot readiness check validates two things for the
Windows-first single-host pilot baseline:

- the delivered bundle contains the expected hashed artifact manifest
- the target host meets the documented prerequisites before bootstrap

The authoritative repo-side entrypoint is:

```bash
python scripts/check_customer_pilot_readiness.py --bundle dist/customer-pilot/etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions.zip
```

For an extracted bundle on Windows, the shipped wrapper is:

```powershell
powershell -ExecutionPolicy Bypass -File .\launch\check_pilot_readiness.ps1
```

## What It Checks

- Windows host platform for the supported pilot baseline
- Python `3.11+`
- Docker CLI availability and Docker daemon reachability
- install-root readiness for the bundle bootstrap path
- free disk space for the documented single-host pilot footprint
- default demo and service port availability
- required bundle files
- `pilot_handoff_manifest.json` integrity against delivered artifact
  hashes

## Outputs

The readiness command prints a JSON summary with:

- overall `status`
- bundle identity (`pilot_name`, `version`)
- per-check results
- warnings
- blocking errors

You can also persist that summary:

```bash
python scripts/check_customer_pilot_readiness.py --bundle dist/customer-pilot/etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions.zip --output dist/customer-pilot/readiness.json
```

## Handoff Manifest

Every packaged customer pilot bundle now includes:

- `pilot_manifest.json`
- `pilot_handoff_manifest.json`

`pilot_handoff_manifest.json` is the delivery-integrity record for the
pilot bundle. It includes:

- project and bundle metadata
- source manifest and seeded run ID
- verification type (`sha256`)
- a hash and size entry for each delivered artifact

The readiness checker replays those hashes against the extracted bundle
or packaged zip so customer handoff can prove which artifact set was
delivered.

## Operator Use

Run the readiness check:

1. before shipping a pilot bundle to confirm the handoff artifact is
   intact
2. on the customer host before running the Windows bootstrap
3. again after copying the bundle if there is any question about file
   integrity

This is a readiness and delivery-integrity control, not the full pilot
acceptance or operator runbook. Those remain separate handoff material
in [customer-pilot-runbooks.md](customer-pilot-runbooks.md) and
[customer-pilot-acceptance-checklist.md](customer-pilot-acceptance-checklist.md).
