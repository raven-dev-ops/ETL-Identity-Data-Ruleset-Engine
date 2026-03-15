from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Sequence
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
PRESERVE_PATHS = (
    Path("runtime") / "pilot_bootstrap.json",
    Path("runtime") / "pilot_runtime.env",
    Path("runtime") / "logs",
    Path("state") / "pipeline_state.sqlite",
)


def _ensure_runtime_src_on_path(bundle_root: Path | None = None) -> None:
    candidate_paths = [REPO_ROOT / "src", REPO_ROOT / "runtime" / "src"]
    if bundle_root is not None:
        candidate_paths.insert(0, bundle_root / "runtime" / "src")
    for candidate in candidate_paths:
        if candidate.exists() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
            return


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Apply a patch upgrade to an extracted customer pilot install while either "
            "preserving the current runtime state or reseeding from the shipped artifacts."
        )
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--source-bundle", default=None, help="New customer pilot bundle zip.")
    source_group.add_argument(
        "--source-bundle-root",
        default=None,
        help="Extracted new customer pilot bundle root.",
    )
    parser.add_argument(
        "--install-root",
        required=True,
        help="Existing extracted customer pilot install root to patch in place.",
    )
    parser.add_argument(
        "--mode",
        choices=("preserve_state", "reseed"),
        default="preserve_state",
        help="Preserve the current bootstrap/runtime state or reseed from the shipped artifacts.",
    )
    parser.add_argument(
        "--python",
        default=None,
        help="Optional Python executable override. Defaults to the install venv python when present.",
    )
    return parser.parse_args(argv)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _copy_path(source: Path, destination: Path) -> None:
    if source.is_dir():
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(source, destination)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _resolve_install_root(install_root: str) -> Path:
    resolved = Path(install_root).resolve()
    if not (resolved / "pilot_manifest.json").exists():
        raise FileNotFoundError(
            "install-root does not point at an extracted customer pilot bundle root"
        )
    return resolved


@contextmanager
def _resolved_source_bundle_root(*, source_bundle: str | None, source_bundle_root: str | None):
    if source_bundle_root:
        resolved = Path(source_bundle_root).resolve()
        if not (resolved / "pilot_manifest.json").exists():
            raise FileNotFoundError(
                "source-bundle-root does not point at an extracted customer pilot bundle root"
            )
        yield resolved
        return

    assert source_bundle is not None
    bundle_path = Path(source_bundle).resolve()
    if not bundle_path.exists():
        raise FileNotFoundError(f"source bundle not found: {bundle_path}")
    with tempfile.TemporaryDirectory(prefix="etl-customer-pilot-upgrade-source-") as temp_dir:
        extracted_root = Path(temp_dir) / "bundle"
        with zipfile.ZipFile(bundle_path) as archive:
            archive.extractall(extracted_root)
        yield extracted_root


def _resolve_python_executable(*, install_root: Path, explicit_python: str | None) -> str:
    if explicit_python:
        return explicit_python
    venv_python = install_root / ".venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def _existing_bootstrap_config(install_root: Path) -> dict[str, object] | None:
    config_path = install_root / "runtime" / "pilot_bootstrap.json"
    if not config_path.exists():
        return None
    return _read_json(config_path)


def _preserve_paths(install_root: Path, staging_root: Path) -> None:
    for relative_path in PRESERVE_PATHS:
        source_path = install_root / relative_path
        if source_path.exists():
            _copy_path(source_path, staging_root / relative_path)


def _overlay_bundle(source_root: Path, install_root: Path) -> None:
    for source_path in sorted(source_root.rglob("*")):
        relative_path = source_path.relative_to(source_root)
        destination = install_root / relative_path
        if source_path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination)


def _restore_preserved_paths(staging_root: Path, install_root: Path) -> None:
    for preserved_path in sorted(staging_root.rglob("*")):
        relative_path = preserved_path.relative_to(staging_root)
        destination = install_root / relative_path
        if preserved_path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(preserved_path, destination)


def _service_status_snapshot(install_root: Path) -> dict[str, dict[str, object]]:
    if sys.platform != "win32":
        return {}
    _ensure_runtime_src_on_path(install_root)
    from etl_identity_engine.windows_pilot_services import query_windows_pilot_service_status

    snapshot: dict[str, dict[str, object]] = {}
    for kind in ("demo_shell", "service_api"):
        try:
            snapshot[kind] = {
                "installed": query_windows_pilot_service_status(kind, bundle_root=install_root).installed,
                "status": query_windows_pilot_service_status(kind, bundle_root=install_root).status,
            }
        except Exception:
            snapshot[kind] = {"installed": False, "status": "unavailable"}
    return snapshot


def _manage_services(install_root: Path, action: str, *, service_kind: str = "all") -> None:
    if sys.platform != "win32":
        return
    _ensure_runtime_src_on_path(install_root)
    from etl_identity_engine.windows_pilot_services import manage_windows_pilot_services

    manage_windows_pilot_services(
        bundle_root=install_root,
        action=action,
        service_kind=service_kind,
    )


def _install_runtime(install_root: Path, python_executable: str) -> None:
    script_root = REPO_ROOT / "scripts"
    if not script_root.exists():
        script_root = install_root / "tools"
    bootstrap_script = script_root / "bootstrap_windows_customer_pilot.py"
    if not bootstrap_script.exists():
        bootstrap_script = install_root / "tools" / "bootstrap_windows_pilot.py"
    if not bootstrap_script.exists():
        raise FileNotFoundError("bootstrap script was not found for the customer pilot install")

    import importlib.util

    spec = importlib.util.spec_from_file_location("bootstrap_windows_customer_pilot_script", bootstrap_script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    venv_python = module.ensure_virtualenv(
        bundle_root=install_root,
        python_executable=python_executable,
    )
    module.install_runtime_requirements(
        venv_python=venv_python,
        bundle_root=install_root,
    )


def _rebuild_demo_shell_from_current_state(install_root: Path, python_executable: str) -> None:
    bootstrap_config = _existing_bootstrap_config(install_root)
    if bootstrap_config is None:
        return
    rebuild_tool = install_root / "tools" / "rebuild_demo_shell.py"
    if not rebuild_tool.exists():
        raise FileNotFoundError(f"rebuild tool not found: {rebuild_tool}")
    subprocess_args = [
        python_executable,
        str(rebuild_tool),
        "--state-db",
        str(bootstrap_config["state_db"]),
        "--run-id",
        str(bootstrap_config["run_id"]),
        "--output-dir",
        str(install_root / "demo_shell"),
        "--prepare-only",
    ]
    import subprocess

    completed = subprocess.run(
        subprocess_args,
        cwd=install_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode != 0:
        detail = "\n".join(part for part in (completed.stdout.strip(), completed.stderr.strip()) if part)
        raise RuntimeError(f"rebuild demo shell failed\n{detail}")


def _reseed_install(install_root: Path, python_executable: str, existing_config: dict[str, object] | None) -> None:
    script_root = REPO_ROOT / "scripts"
    bootstrap_script = script_root / "bootstrap_windows_customer_pilot.py"
    if not bootstrap_script.exists():
        bootstrap_script = install_root / "tools" / "bootstrap_windows_pilot.py"
    if not bootstrap_script.exists():
        raise FileNotFoundError("bootstrap script was not found for reseed")
    import importlib.util

    spec = importlib.util.spec_from_file_location("bootstrap_windows_customer_pilot_script", bootstrap_script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    postgres_port = None if existing_config is None else int(existing_config.get("postgres_port", 0) or 0)
    postgres_container_name = None if existing_config is None else str(existing_config.get("postgres_container_name", "") or "")
    postgres_db = "identity_state" if existing_config is None else str(existing_config.get("postgres_db", "identity_state"))
    postgres_user = "etl_identity" if existing_config is None else str(existing_config.get("postgres_user", "etl_identity"))
    postgres_password = "pilot-password" if existing_config is None else str(existing_config.get("postgres_password", "pilot-password"))
    demo_host = "127.0.0.1" if existing_config is None else str(existing_config.get("demo_host", "127.0.0.1"))
    demo_port = 8000 if existing_config is None else int(existing_config.get("demo_port", 8000) or 8000)
    service_port = 8010 if existing_config is None else int(existing_config.get("service_port", 8010) or 8010)
    module.bootstrap_windows_customer_pilot(
        bundle=None,
        bundle_root=str(install_root),
        install_root=None,
        python_executable=python_executable,
        postgres_port=postgres_port or None,
        postgres_container_name=postgres_container_name or None,
        postgres_db=postgres_db,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        demo_host=demo_host,
        demo_port=demo_port,
        service_port=service_port,
        prepare_only=True,
    )


def patch_upgrade_customer_pilot(
    *,
    install_root: Path,
    source_root: Path,
    mode: str,
    python_executable: str,
) -> dict[str, Any]:
    existing_config = _existing_bootstrap_config(install_root)
    service_snapshot = _service_status_snapshot(install_root)
    running_kinds = tuple(
        kind
        for kind, status in service_snapshot.items()
        if status.get("installed") and status.get("status") == "running"
    )
    if running_kinds:
        _manage_services(install_root, "stop")

    with tempfile.TemporaryDirectory(prefix="etl-customer-pilot-upgrade-preserve-") as temp_dir:
        preserved_root = Path(temp_dir) / "preserved"
        preserved_root.mkdir(parents=True, exist_ok=True)
        if mode == "preserve_state":
            _preserve_paths(install_root, preserved_root)
        _overlay_bundle(source_root, install_root)
        if mode == "preserve_state":
            _restore_preserved_paths(preserved_root, install_root)

    _install_runtime(install_root, python_executable)
    runtime_python = (
        str(install_root / ".venv" / "Scripts" / "python.exe")
        if (install_root / ".venv" / "Scripts" / "python.exe").exists()
        else python_executable
    )
    if mode == "preserve_state":
        _rebuild_demo_shell_from_current_state(install_root, runtime_python)
    else:
        _reseed_install(install_root, runtime_python, existing_config)

    if running_kinds:
        _manage_services(install_root, "start")

    return {
        "install_root": str(install_root),
        "source_root": str(source_root),
        "mode": mode,
        "restarted_services": list(running_kinds),
        "service_snapshot_before": service_snapshot,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    install_root = _resolve_install_root(args.install_root)
    with _resolved_source_bundle_root(
        source_bundle=args.source_bundle,
        source_bundle_root=args.source_bundle_root,
    ) as source_root:
        summary = patch_upgrade_customer_pilot(
            install_root=install_root,
            source_root=source_root,
            mode=args.mode,
            python_executable=_resolve_python_executable(
                install_root=install_root,
                explicit_python=args.python,
            ),
        )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
