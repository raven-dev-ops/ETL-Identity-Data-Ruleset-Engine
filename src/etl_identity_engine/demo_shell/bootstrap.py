from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_OUTPUT_DIR = Path("dist") / "public-safety-demo-django"


@dataclass(frozen=True)
class PreparedDemoShell:
    output_dir: Path
    db_path: Path
    bundle_root: Path
    bundle_path: Path
    base_url: str
    source_kind: str
    source_run_id: str | None = None


def configure_demo_shell_environment(
    *,
    output_dir: Path,
    db_path: Path | None = None,
    bundle_root: Path | None = None,
) -> tuple[Path, Path]:
    resolved_output_dir = output_dir.resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_db_path = (db_path or (resolved_output_dir / "db.sqlite3")).resolve()
    resolved_bundle_root = (bundle_root or (resolved_output_dir / "bundle")).resolve()
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "etl_identity_engine.demo_shell.settings")
    os.environ["PUBLIC_SAFETY_DEMO_BASE_DIR"] = str(resolved_output_dir)
    os.environ["PUBLIC_SAFETY_DEMO_DB"] = str(resolved_db_path)
    os.environ["PUBLIC_SAFETY_DEMO_BUNDLE_ROOT"] = str(resolved_bundle_root)
    os.environ.setdefault("PUBLIC_SAFETY_DEMO_SECRET_KEY", "public-safety-demo-only")
    os.environ.setdefault("PUBLIC_SAFETY_DEMO_ALLOWED_HOSTS", "127.0.0.1,localhost,testserver")
    try:
        from django.conf import settings as django_settings
        from django.db import connections
    except Exception:
        return resolved_db_path, resolved_bundle_root

    if django_settings.configured:
        django_settings.DATABASES["default"]["NAME"] = str(resolved_db_path)
        django_settings.PUBLIC_SAFETY_DEMO_BUNDLE_ROOT = resolved_bundle_root
        django_settings.BASE_DIR = resolved_output_dir
        django_settings.ALLOWED_HOSTS = [
            host.strip()
            for host in os.environ["PUBLIC_SAFETY_DEMO_ALLOWED_HOSTS"].split(",")
            if host.strip()
        ]
        connections.close_all()
        connections.databases["default"]["NAME"] = str(resolved_db_path)
    return resolved_db_path, resolved_bundle_root


def prepare_public_safety_demo_shell(
    *,
    bundle_path: Path | None = None,
    state_db: str | Path | None = None,
    run_id: str | None = None,
    output_dir: Path,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> PreparedDemoShell:
    if (bundle_path is None) == (state_db is None):
        raise ValueError("Exactly one of bundle_path or state_db must be provided")
    db_path, bundle_root = configure_demo_shell_environment(output_dir=output_dir)

    import django
    from django.core.management import call_command

    django.setup()
    from etl_identity_engine.demo_shell.loader import (
        load_public_safety_demo_bundle,
        load_public_safety_demo_state,
    )

    call_command("migrate", interactive=False, verbosity=0)
    if bundle_path is not None:
        load_public_safety_demo_bundle(bundle_path=bundle_path, extract_dir=bundle_root)
        source_path = bundle_path.resolve()
        source_kind = "bundle"
        source_run_id = None
    else:
        loaded = load_public_safety_demo_state(
            state_db=state_db,
            extract_dir=bundle_root,
            run_id=run_id,
        )
        source_path = loaded.bundle_path
        source_kind = "persisted_state"
        source_run_id = loaded.demo_run.summary.get("source_run_id") or run_id
    return PreparedDemoShell(
        output_dir=output_dir.resolve(),
        db_path=db_path,
        bundle_root=bundle_root,
        bundle_path=source_path,
        base_url=f"http://{host}:{port}/",
        source_kind=source_kind,
        source_run_id=None if not source_run_id else str(source_run_id),
    )


def run_public_safety_demo_server(*, host: str = "127.0.0.1", port: int = 8000) -> None:
    import django
    from django.core.management import call_command

    django.setup()
    call_command("runserver", f"{host}:{port}", use_reloader=False)
