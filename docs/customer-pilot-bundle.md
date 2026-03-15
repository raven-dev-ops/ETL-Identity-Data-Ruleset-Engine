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

To emit a detached signature for the handoff manifest, add a trusted
Ed25519 private key:

```bash
python scripts/package_customer_pilot_bundle.py --output-dir dist/customer-pilot --signing-key C:\keys\etl-identity-engine-signing-private.pem --signer-identity "pilot-release@example.test" --key-id "pilot-ed25519"
```

To wrap the delivered pilot bundle in the encrypted handoff format, add
one encryption secret input:

```bash
python scripts/package_customer_pilot_bundle.py --output-dir dist/customer-pilot --passphrase-file C:\secrets\pilot-bundle-passphrase.txt
```

You can also point at a different seeded manifest:

```bash
python scripts/package_customer_pilot_bundle.py --manifest fixtures/public_safety_regressions/manifest.yml --pilot-name public-safety-regressions --output-dir dist/customer-pilot
```

The script writes a deterministic bundle name like:

`etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions.zip`

When encryption is enabled, the output bundle name is:

`etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions-encrypted.zip`

## Bundle Contents

The packaged zip includes:

- `README.md`
- `pilot_manifest.json`
- `pilot_handoff_manifest.json`
- `pilot_handoff_manifest.sig.json` when signing is enabled
- `seed_dataset/`
- `seed_run/data/`
- `state/pipeline_state.sqlite`
- `demo_shell/`
- `runtime/`
- `tools/rebuild_demo_shell.py`
- `tools/bootstrap_windows_pilot.py`
- `tools/check_pilot_readiness.py`
- `tools/manage_windows_pilot_services.py`
- `tools/package_customer_pilot_support_bundle.py`
- `tools/patch_upgrade_customer_pilot.py`
- `tools/verify_handoff_signature.py`
- `launch/start_demo_shell.ps1`
- `launch/start_demo_shell.sh`
- `launch/bootstrap_windows_pilot.ps1`
- `launch/check_pilot_readiness.ps1`
- `launch/manage_pilot_services.ps1`
- `launch/collect_support_bundle.ps1`
- `launch/patch_upgrade_pilot.ps1`

## Local Walkthrough

From the extracted bundle root:

If the delivered pilot bundle is encrypted, decrypt it first:

```bash
python scripts/restore_encrypted_bundle.py --bundle dist/customer-pilot/etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions-encrypted.zip --output-dir dist/customer-pilot/extracted --passphrase-file C:\secrets\pilot-bundle-passphrase.txt
```

1. Run the readiness check first:
   `powershell -ExecutionPolicy Bypass -File .\launch\check_pilot_readiness.ps1`
   If the bundle includes `pilot_handoff_manifest.sig.json`, also pass
   `-TrustedPublicKey <path-to-public-key.pem>` or set
   `ETL_IDENTITY_TRUSTED_SIGNER_PUBLIC_KEY`.
2. For the supported Windows-first PostgreSQL pilot path, run:
   `powershell -ExecutionPolicy Bypass -File .\launch\bootstrap_windows_pilot.ps1`
3. For the portable seeded SQLite walkthrough, install the shipped
   runtime dependencies:
   `python -m pip install -r runtime/requirements-pilot.txt`
4. Start the local SQLite walkthrough:
   - PowerShell: `./launch/start_demo_shell.ps1`
   - Bash: `./launch/start_demo_shell.sh`

For the supported Windows single-host PostgreSQL pilot, operators can
also:

- install or manage Windows services:
  `powershell -ExecutionPolicy Bypass -File .\launch\manage_pilot_services.ps1 -Action status`
- collect a redacted troubleshooting artifact:
  `powershell -ExecutionPolicy Bypass -File .\launch\collect_support_bundle.ps1`
- apply a preserve-state or reseed patch upgrade:
  `powershell -ExecutionPolicy Bypass -File .\launch\patch_upgrade_pilot.ps1 -SourceBundle C:\handoff\etl-identity-engine-vX.Y.Z-customer-pilot-example.zip -Mode preserve_state`

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
  hashed handoff-manifest readiness check plus optional detached
  signature verification, but it does not replace the operator runbook
  and acceptance material documented in
  [customer-pilot-runbooks.md](customer-pilot-runbooks.md) and
  [customer-pilot-acceptance-checklist.md](customer-pilot-acceptance-checklist.md).
