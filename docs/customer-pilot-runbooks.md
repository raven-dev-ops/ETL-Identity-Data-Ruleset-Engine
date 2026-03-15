# Customer Pilot Runbooks

These runbooks cover the supported single-host customer pilot baseline:

- extracted customer pilot bundle
- Windows host
- local Python `3.11+`
- Docker Desktop
- local PostgreSQL container
- Django demo shell and optional service API

They are intentionally limited to the supported pilot topology. They are
not production operations procedures.

## Install

From the extracted bundle root:

If the delivered pilot bundle is the encrypted handoff form, decrypt it
with `scripts/restore_encrypted_bundle.py` before these steps.

1. Run the readiness check:

```powershell
powershell -ExecutionPolicy Bypass -File .\launch\check_pilot_readiness.ps1
```

2. Bootstrap the pilot environment:

```powershell
powershell -ExecutionPolicy Bypass -File .\launch\bootstrap_windows_pilot.ps1 --prepare-only
```

3. Confirm the generated files exist:

- `runtime/pilot_bootstrap.json`
- `runtime/pilot_runtime.env`
- `launch/start_pilot_demo_shell.ps1`
- `launch/start_pilot_service.ps1`
- `launch/manage_pilot_services.ps1`
- `launch/collect_support_bundle.ps1`
- `launch/patch_upgrade_pilot.ps1`
- `launch/stop_pilot_postgres.ps1`

## Startup

### Demo Shell

Start the PostgreSQL-backed Django demo shell:

```powershell
.\launch\start_pilot_demo_shell.ps1
```

Default URL:

- `http://127.0.0.1:8000/`

### Service API

Start the authenticated service API:

```powershell
.\launch\start_pilot_service.ps1
```

Default URL:

- `http://127.0.0.1:8010/`

Default API keys written by the bootstrap:

- reader: `pilot-reader-key`
- operator: `pilot-operator-key`

### Windows Services

Install and start the supported Windows services for both surfaces:

```powershell
.\launch\manage_pilot_services.ps1 -Action install-and-start -ServiceKind all
```

Check the current Windows service state:

```powershell
.\launch\manage_pilot_services.ps1 -Action status -ServiceKind all
```

Stop and remove the services before a rollback or manual cleanup:

```powershell
.\launch\manage_pilot_services.ps1 -Action stop-and-remove -ServiceKind all
```

The supported service wrappers are only for the Windows single-host
pilot baseline. They assume the extracted bundle root remains in place,
the pilot `.venv` exists, and the generated bootstrap/runtime files
remain under `runtime/`.

## Operator Console

After the service API is running, operators can use the minimal admin
console at:

- `http://127.0.0.1:8010/admin/console`

The console is read-only. It requires operator authentication with the
documented `service:metrics` and `audit_events:read` scopes and surfaces
health, readiness-style metrics, and recent persisted audit events for
the single-host pilot.

## Rollback

For the supported pilot rollback, return to the shipped bundle state:

1. Stop and remove the Windows service wrappers if they were installed:

```powershell
.\launch\manage_pilot_services.ps1 -Action stop-and-remove -ServiceKind all
```

2. Stop and remove the PostgreSQL pilot container:

```powershell
.\launch\stop_pilot_postgres.ps1
```

3. Close any open demo shell or service windows.
4. Delete the extracted working bundle directory.
5. Re-extract the original customer pilot zip.
6. Rerun:

```powershell
powershell -ExecutionPolicy Bypass -File .\launch\check_pilot_readiness.ps1
powershell -ExecutionPolicy Bypass -File .\launch\bootstrap_windows_pilot.ps1 --prepare-only
```

This is the supported rollback path because the pilot is deterministic
and seeded from the shipped manifest.

## Backup

For the pilot baseline, back up these artifacts before making local
changes:

- the original customer pilot zip
- `pilot_manifest.json`
- `pilot_handoff_manifest.json`
- `runtime/pilot_bootstrap.json`
- `runtime/pilot_runtime.env`

If the PostgreSQL bootstrap path has been used, prefer the encrypted
persisted-state backup workflow for the pilot database plus replay
bundle attachments:

```bash
python -m etl_identity_engine.cli backup-state-bundle \
  --state-db postgresql://etl_identity:etl_identity@127.0.0.1:15433/identity_state \
  --output backups/pilot_state_backup_encrypted.zip \
  --include-path runtime/replay_bundles \
  --passphrase-file C:\secrets\pilot-state-backup-passphrase.txt
```

If you used a different PostgreSQL host, port, or database name, read
them from `runtime/pilot_bootstrap.json`.

## Support Bundle

Generate a redacted troubleshooting bundle before escalation, upgrade,
or teardown when the pilot behavior differs from the shipped seeded
walkthrough:

```powershell
.\launch\collect_support_bundle.ps1
```

That workflow packages:

- the pilot and handoff manifests
- redacted runtime config and env material
- redacted local logs
- persisted state metadata
- recent runs and audit events
- Windows service status when available

The output zip lands under `dist/customer-pilot-support/` by default.
Share that artifact instead of copying logs and runtime files
piecemeal.

## Patch Upgrade And Reseed

Apply a new delivered pilot bundle over the current extracted install:

```powershell
.\launch\patch_upgrade_pilot.ps1 -SourceBundle C:\handoff\etl-identity-engine-vX.Y.Z-customer-pilot-example.zip -Mode preserve_state
```

Supported modes:

- `preserve_state`
  - keeps the current `runtime/pilot_bootstrap.json`,
    `runtime/pilot_runtime.env`, logs, and persisted SQLite state
  - reinstalls the shipped runtime and rebuilds the demo shell from the
    current state
- `reseed`
  - replaces the install with the new shipped bundle and reruns the
    supported bootstrap path using the prior runtime settings where
    possible

Use `reseed` when you want the install to return to the delivered seeded
state rather than carry forward local changes. After either mode, rerun
the readiness check and either restart the Windows services or start the
demo shell and API manually.

## Demo Execution

Recommended walkthrough order:

1. Show `pilot_handoff_manifest.json` and the readiness check output.
2. Start the demo shell with `.\launch\start_pilot_demo_shell.ps1`.
3. Open the overview page and highlight:
   - linked golden people
   - cross-system CAD/RMS activity
   - seeded scenarios
4. Walk the `CAD And RMS On One Identity` scenario.
5. Open a golden-person detail page and show linked incidents.
6. If needed, start the service API and demonstrate a persisted
   read-model lookup.

## Troubleshooting

- If bootstrap fails before PostgreSQL is ready, confirm Docker Desktop
  is running and rerun the bootstrap.
- If the readiness check fails on ports, pick alternate ports for the
  demo shell or service startup.
- If the extracted bundle root was modified, re-extract from the
  original zip and rerun the readiness check.
- If support needs repro details, generate the support bundle before
  deleting the extracted install or reseeding it.
