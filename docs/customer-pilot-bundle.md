# Customer Pilot Bundle

The customer pilot bundle is the standalone handoff package for a local
public-safety walkthrough.

It is built from a seeded CAD/RMS manifest-backed run, persists that
run into SQLite state, prepares the read-only Django demo shell, and
ships the launch helpers needed for a local operator or buyer demo.

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
- `seed_dataset/`
- `seed_run/data/`
- `state/pipeline_state.sqlite`
- `demo_shell/`
- `runtime/`
- `tools/rebuild_demo_shell.py`
- `launch/start_demo_shell.ps1`
- `launch/start_demo_shell.sh`

## Local Walkthrough

From the extracted bundle root:

1. Create and activate a Python `3.11+` virtual environment.
2. Install the shipped runtime dependencies:
   `python -m pip install -r runtime/requirements-pilot.txt`
3. Start the local walkthrough:
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
- It does not replace the Windows-first bootstrap, readiness check, or
  signed handoff work tracked later in the `v1.2.0` backlog.
