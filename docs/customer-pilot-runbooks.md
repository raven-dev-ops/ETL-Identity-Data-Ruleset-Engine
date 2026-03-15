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

## Rollback

For the supported pilot rollback, return to the shipped bundle state:

1. Stop and remove the PostgreSQL pilot container:

```powershell
.\launch\stop_pilot_postgres.ps1
```

2. Close any open demo shell or service windows.
3. Delete the extracted working bundle directory.
4. Re-extract the original customer pilot zip.
5. Rerun:

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
