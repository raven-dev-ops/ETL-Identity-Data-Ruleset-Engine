# Windows Pilot Bootstrap

This is the supported Windows-first bootstrap path for the single-host
customer pilot baseline.

The supported topology is:

- extracted customer pilot bundle
- local Python `3.11+`
- Docker Desktop
- PostgreSQL in a local single-host container
- Django demo shell rebuilt from that PostgreSQL state

## Supported Entry Point

From the extracted customer pilot bundle root:

```powershell
powershell -ExecutionPolicy Bypass -File .\launch\check_pilot_readiness.ps1
powershell -ExecutionPolicy Bypass -File .\launch\bootstrap_windows_pilot.ps1 --prepare-only
```

If the bundle includes `pilot_handoff_manifest.sig.json`, pass the
trusted Ed25519 public key during the readiness step:

```powershell
powershell -ExecutionPolicy Bypass -File .\launch\check_pilot_readiness.ps1 -TrustedPublicKey C:\keys\etl-identity-engine-signing-public.pem
```

You can also set `ETL_IDENTITY_TRUSTED_SIGNER_PUBLIC_KEY` before
running the wrapper.

That bootstrap path:

1. creates a local `.venv`
2. installs the bundled runtime dependencies
3. starts or reuses a local PostgreSQL container
4. upgrades the PostgreSQL state store
5. reruns the seeded manifest into PostgreSQL
6. rebuilds the Django demo shell from that PostgreSQL state
7. writes local runtime config and launcher files

To bootstrap and immediately start the demo shell:

```powershell
powershell -ExecutionPolicy Bypass -File .\launch\bootstrap_windows_pilot.ps1
```

## Generated Runtime Files

After bootstrap, the bundle root includes:

- `runtime/pilot_runtime.env`
- `runtime/pilot_bootstrap.json`
- `launch/start_pilot_demo_shell.ps1`
- `launch/start_pilot_service.ps1`
- `launch/stop_pilot_postgres.ps1`

The readiness check, hashed handoff manifest, and optional detached
signature workflow are documented in
[customer-pilot-readiness.md](customer-pilot-readiness.md).

## Follow-On Commands

Start the PostgreSQL-backed Django walkthrough again later:

```powershell
.\launch\start_pilot_demo_shell.ps1
```

Start the authenticated service API against the same PostgreSQL state:

```powershell
.\launch\start_pilot_service.ps1
```

Stop and remove the local PostgreSQL pilot container:

```powershell
.\launch\stop_pilot_postgres.ps1
```

## Scope Boundary

This bootstrap is the supported single-host pilot path for evaluation
and walkthroughs. It is not a production installer or HA topology.
