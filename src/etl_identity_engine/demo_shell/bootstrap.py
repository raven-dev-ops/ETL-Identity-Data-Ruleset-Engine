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
    return resolved_db_path, resolved_bundle_root


def prepare_public_safety_demo_shell(
    *,
    bundle_path: Path,
    output_dir: Path,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> PreparedDemoShell:
    db_path, bundle_root = configure_demo_shell_environment(output_dir=output_dir)

    import django
    from django.core.management import call_command

    django.setup()
    call_command("migrate", interactive=False, verbosity=0)
    call_command(
        "load_public_safety_demo_bundle",
        "--bundle",
        str(bundle_path),
        "--extract-dir",
        str(bundle_root),
        "--replace",
        verbosity=0,
    )
    return PreparedDemoShell(
        output_dir=output_dir.resolve(),
        db_path=db_path,
        bundle_root=bundle_root,
        bundle_path=bundle_path.resolve(),
        base_url=f"http://{host}:{port}/",
    )


def run_public_safety_demo_server(*, host: str = "127.0.0.1", port: int = 8000) -> None:
    import django
    from django.core.management import call_command

    django.setup()
    call_command("runserver", f"{host}:{port}", use_reloader=False)
