# Customer Pilot Bundle

The customer pilot bundle is the standalone handoff package for a local
public-safety walkthrough.

It is built from a seeded CAD/RMS manifest-backed run, persists that
run into SQLite state, prepares the read-only Django demo shell, and
ships the launch helpers needed for a local operator or buyer demo.
It now also includes the Windows-first bootstrap needed to rebuild the
same seeded pilot into a local PostgreSQL-backed single-host runtime.
It also now includes a hashed handoff manifest and readiness-check path
for customer-environment validation before bootstrap.

## Build Command

Package the default seeded pilot bundle with:

```bash
python scripts/package_customer_pilot_bundle.py --output-dir dist/customer-pilot
```

You can also point at a different seeded manifest:

```bash
python scripts/package_customer_pilot_bundle.py --manifest fixtures/public_safety_regressions/manifest.yml --pilot-name public-safety-regressions --output-dir dist/customer-pilot
```

The script writes a deterministic bundle name like:

`etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions.zip`

## Bundle Contents

The packaged zip includes:

- `README.md`
- `pilot_manifest.json`
- `pilot_handoff_manifest.json`
- `seed_dataset/`
- `seed_run/data/`
- `state/pipeline_state.sqlite`
- `demo_shell/`
- `runtime/`
- `tools/rebuild_demo_shell.py`
- `tools/bootstrap_windows_pilot.py`
- `tools/check_pilot_readiness.py`
- `launch/start_demo_shell.ps1`
- `launch/start_demo_shell.sh`
- `launch/bootstrap_windows_pilot.ps1`
- `launch/check_pilot_readiness.ps1`

## Local Walkthrough

From the extracted bundle root:

1. Run the readiness check first:
   `powershell -ExecutionPolicy Bypass -File .\launch\check_pilot_readiness.ps1`
2. For the supported Windows-first PostgreSQL pilot path, run:
   `powershell -ExecutionPolicy Bypass -File .\launch\bootstrap_windows_pilot.ps1`
3. For the portable seeded SQLite walkthrough, install the shipped
   runtime dependencies:
   `python -m pip install -r runtime/requirements-pilot.txt`
4. Start the local SQLite walkthrough:
   - PowerShell: `./launch/start_demo_shell.ps1`
   - Bash: `./launch/start_demo_shell.sh`

The default walkthrough URL is `http://127.0.0.1:8000/`.

If you only want to rebuild the demo shell workspace without starting
the server:

```bash
python tools/rebuild_demo_shell.py --prepare-only
```

## Scope Boundary

This bundle is a seeded customer pilot handoff, not a production
deployment package.

- It is designed for local walkthroughs.
- It uses synthetic public-safety data only.
- It now includes the Windows-first single-host bootstrap path and the
  hashed handoff-manifest readiness check, but it does not replace the
  operator runbook and acceptance material documented in
  [customer-pilot-runbooks.md](customer-pilot-runbooks.md) and
  [customer-pilot-acceptance-checklist.md](customer-pilot-acceptance-checklist.md).
