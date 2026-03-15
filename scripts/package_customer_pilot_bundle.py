from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Sequence
import zipfile

import tomllib

from package_release_sample import (
    PROJECT_NAME,
    REPO_ROOT,
    _build_pythonpath,
    _ensure_repo_src_on_path,
    _zip_entry_timestamp,
    read_project_version,
    resolve_generated_at_utc,
    resolve_output_dir,
    resolve_source_commit,
)


DEFAULT_OUTPUT_DIR = Path("dist") / "customer-pilot"
DEFAULT_SOURCE_MANIFEST = REPO_ROOT / "fixtures" / "public_safety_regressions" / "manifest.yml"
MANIFEST_NAME = "pilot_manifest.json"
HANDOFF_MANIFEST_NAME = "pilot_handoff_manifest.json"
HANDOFF_SIGNATURE_NAME = "pilot_handoff_manifest.sig.json"


def _prepare_public_safety_demo_shell(**kwargs: object) -> object:
    _ensure_repo_src_on_path()
    from etl_identity_engine.demo_shell.bootstrap import prepare_public_safety_demo_shell

    return prepare_public_safety_demo_shell(**kwargs)


def _pipeline_state_store(state_db: Path) -> object:
    _ensure_repo_src_on_path()
    from etl_identity_engine.storage.sqlite_store import PipelineStateStore

    return PipelineStateStore(state_db)


def _close_django_connections() -> None:
    try:
        from django.db import connections
    except Exception:
        return
    connections.close_all()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Package a standalone seeded public-safety customer pilot bundle."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the packaged customer pilot zip will be written.",
    )
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_SOURCE_MANIFEST),
        help="Seed public-safety manifest to package into the customer pilot bundle.",
    )
    parser.add_argument(
        "--pilot-name",
        default=None,
        help="Name to embed in the bundle filename and manifest. Defaults to the source manifest directory name.",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Version to embed in the bundle name and manifest. Defaults to pyproject.toml.",
    )
    parser.add_argument(
        "--signing-key",
        default=None,
        help="Optional Ed25519 private key PEM used to emit a detached handoff-manifest signature.",
    )
    parser.add_argument(
        "--signer-identity",
        default=None,
        help="Optional signer identity to record in the detached signature metadata.",
    )
    parser.add_argument(
        "--key-id",
        default=None,
        help="Optional key identifier to record in the detached signature metadata.",
    )
    return parser.parse_args(argv)


def sanitize_pilot_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-")
    if not normalized:
        raise ValueError("pilot name must contain at least one letter or number")
    return normalized


def build_bundle_name(version: str, pilot_name: str) -> str:
    return f"etl-identity-engine-v{version}-customer-pilot-{pilot_name}.zip"


def build_manifest(
    *,
    version: str,
    pilot_name: str,
    generated_at_utc: str,
    source_commit: str,
    source_manifest: str,
    source_run_id: str,
    state_db: str,
    demo_shell_dir: str,
    launch_helpers: Sequence[str],
    artifacts: Sequence[str],
) -> dict[str, object]:
    return {
        "project": PROJECT_NAME,
        "bundle_type": "customer_pilot",
        "version": version,
        "pilot_name": pilot_name,
        "generated_at_utc": generated_at_utc,
        "source_commit": source_commit,
        "source_manifest": source_manifest,
        "source_run_id": source_run_id,
        "state_db": state_db,
        "demo_shell_dir": demo_shell_dir,
        "launch_helpers": list(launch_helpers),
        "artifacts": list(artifacts),
    }


def build_handoff_manifest(
    *,
    version: str,
    pilot_name: str,
    generated_at_utc: str,
    source_commit: str,
    source_manifest: str,
    source_run_id: str,
    verification_type: str,
    artifacts: Sequence[dict[str, object]],
) -> dict[str, object]:
    return {
        "project": PROJECT_NAME,
        "bundle_type": "customer_pilot",
        "version": version,
        "pilot_name": pilot_name,
        "generated_at_utc": generated_at_utc,
        "source_commit": source_commit,
        "source_manifest": source_manifest,
        "source_run_id": source_run_id,
        "verification_type": verification_type,
        "artifacts": list(artifacts),
    }


def _read_project_dependencies(pyproject_path: Path = REPO_ROOT / "pyproject.toml") -> tuple[str, ...]:
    payload = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    dependencies = payload.get("project", {}).get("dependencies", [])
    return tuple(str(item).strip() for item in dependencies if str(item).strip())


def _copy_tree_files(source_root: Path, destination_root: Path) -> None:
    for source_path in sorted(source_root.rglob("*")):
        if source_path.is_dir():
            continue
        if "__pycache__" in source_path.parts or source_path.suffix == ".pyc":
            continue
        relative_path = source_path.relative_to(source_root)
        destination_path = destination_root / relative_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _artifact_hash_entries(
    staging_root: Path,
    *,
    exclude_names: Sequence[str] = (),
) -> tuple[dict[str, object], ...]:
    excluded = set(exclude_names)
    entries: list[dict[str, object]] = []
    for path in sorted(staging_root.rglob("*")):
        if not path.is_file() or path.name in excluded:
            continue
        payload = path.read_bytes()
        entries.append(
            {
                "path": path.relative_to(staging_root).as_posix(),
                "sha256": _sha256_bytes(payload),
                "size_bytes": len(payload),
            }
        )
    return tuple(entries)


def _write_detached_signature(
    *,
    destination: Path,
    manifest_path: str,
    manifest_bytes: bytes,
    private_key_path: Path,
    signer_identity: str | None,
    key_id: str | None,
) -> dict[str, str]:
    _ensure_repo_src_on_path()
    from etl_identity_engine.handoff_signing import write_detached_signature

    return write_detached_signature(
        destination=destination,
        manifest_path=manifest_path,
        manifest_bytes=manifest_bytes,
        private_key_path=private_key_path,
        signer_identity=signer_identity,
        key_id=key_id,
    )


def _write_pilot_readme(*, destination: Path, pilot_name: str, version: str, source_run_id: str) -> None:
    destination.write_text(
        "\n".join(
            [
                "# Customer Pilot Bundle",
                "",
                f"- Pilot name: `{pilot_name}`",
                f"- Project version: `{version}`",
                f"- Seeded run ID: `{source_run_id}`",
                "",
                "## Contents",
                "",
                "- `seed_dataset/`: the seeded CAD/RMS manifest and bundle inputs used for this pilot",
                "- `seed_run/data/`: raw pipeline outputs from the seeded run",
                "- `state/pipeline_state.sqlite`: persisted pipeline state for the seeded run",
                "- `demo_shell/`: the prepared Django + SQLite demo shell workspace",
                "- `runtime/`: ETL Identity Engine source payload, config, and dependency list",
                "- `launch/`: quick startup helpers for local walkthroughs",
                "- `tools/rebuild_demo_shell.py`: rebuild the demo shell from the shipped persisted state",
                "- `tools/bootstrap_windows_pilot.py`: Windows-first bootstrap for the PostgreSQL-backed single-host pilot path",
                "- `tools/check_pilot_readiness.py`: validates the target host, handoff hashes, and an optional detached handoff signature",
                "- `tools/verify_handoff_signature.py`: verifies detached handoff signatures using a trusted Ed25519 public key",
                "- `pilot_handoff_manifest.json`: hashed manifest of the delivered pilot artifacts",
                "- `pilot_handoff_manifest.sig.json`: optional detached signature for the handoff manifest",
                "",
                "## Quick Start",
                "",
                "1. Install Python 3.11 or newer.",
                "2. Run the readiness check before bootstrap:",
                "   `powershell -ExecutionPolicy Bypass -File .\\launch\\check_pilot_readiness.ps1`",
                "   If the bundle includes `pilot_handoff_manifest.sig.json`, also supply",
                "   `-TrustedPublicKey <path-to-trusted-public-key.pem>` or set",
                "   `ETL_IDENTITY_TRUSTED_SIGNER_PUBLIC_KEY` before running the check.",
                "3. On Windows with Docker Desktop available, run:",
                "   `powershell -ExecutionPolicy Bypass -File .\\launch\\bootstrap_windows_pilot.ps1`",
                "4. Or use the portable seeded SQLite walkthrough:",
                "   - Windows PowerShell: `./launch/start_demo_shell.ps1`",
                "   - Bash: `./launch/start_demo_shell.sh`",
                "",
                "If you need to rebuild the shell workspace without starting the server, run:",
                "",
                "`python tools/rebuild_demo_shell.py --prepare-only`",
                "",
                "The default local demo URL is `http://127.0.0.1:8000/`.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_runtime_payload(*, destination_root: Path) -> None:
    runtime_root = destination_root / "runtime"
    (runtime_root / "src").mkdir(parents=True, exist_ok=True)
    _copy_tree_files(REPO_ROOT / "src" / "etl_identity_engine", runtime_root / "src" / "etl_identity_engine")
    _copy_tree_files(REPO_ROOT / "config", runtime_root / "config")
    (runtime_root / "manage_public_safety_demo.py").write_text(
        """from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RUNTIME_SRC = ROOT / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from etl_identity_engine.demo_shell.bootstrap import configure_demo_shell_environment


def main() -> None:
    output_dir = Path(os.environ.get("PUBLIC_SAFETY_DEMO_BASE_DIR", ROOT.parent / "demo_shell"))
    configure_demo_shell_environment(output_dir=output_dir)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "etl_identity_engine.demo_shell.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
""",
        encoding="utf-8",
    )
    shutil.copy2(REPO_ROOT / "LICENSE", runtime_root / "LICENSE")
    shutil.copy2(REPO_ROOT / "pyproject.toml", runtime_root / "pyproject.toml")
    (runtime_root / "requirements-pilot.txt").write_text(
        "\n".join(_read_project_dependencies()) + "\n",
        encoding="utf-8",
    )


def _write_rebuild_tool(*, destination_root: Path) -> None:
    tool_path = destination_root / "tools" / "rebuild_demo_shell.py"
    tool_path.parent.mkdir(parents=True, exist_ok=True)
    tool_path.write_text(
        """from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = ROOT / "runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from etl_identity_engine.demo_shell.bootstrap import prepare_public_safety_demo_shell, run_public_safety_demo_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild or serve the customer pilot demo shell from the shipped persisted state."
    )
    parser.add_argument("--state-db", default=str(ROOT / "state" / "pipeline_state.sqlite"))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-dir", default=str(ROOT / "demo_shell"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8000, type=int)
    parser.add_argument("--prepare-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prepared = prepare_public_safety_demo_shell(
        state_db=args.state_db,
        run_id=args.run_id,
        output_dir=Path(args.output_dir),
        host=args.host,
        port=args.port,
    )
    print(f"customer pilot source run: {prepared.source_run_id}")
    print(f"customer pilot state db: {args.state_db}")
    print(f"customer pilot shell db: {prepared.db_path}")
    print(f"customer pilot artifact root: {prepared.bundle_root}")
    print(f"customer pilot URL: {prepared.base_url}")
    if args.prepare_only:
        return 0
    run_public_safety_demo_server(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
""",
        encoding="utf-8",
    )


def _write_windows_bootstrap_tool(*, destination_root: Path) -> None:
    tool_path = destination_root / "tools" / "bootstrap_windows_pilot.py"
    tool_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(REPO_ROOT / "scripts" / "bootstrap_windows_customer_pilot.py", tool_path)


def _write_readiness_tool(*, destination_root: Path) -> None:
    tool_path = destination_root / "tools" / "check_pilot_readiness.py"
    tool_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(REPO_ROOT / "scripts" / "check_customer_pilot_readiness.py", tool_path)


def _write_signature_tool(*, destination_root: Path) -> None:
    tool_path = destination_root / "tools" / "verify_handoff_signature.py"
    tool_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(REPO_ROOT / "scripts" / "verify_handoff_signature.py", tool_path)


def _write_launch_helpers(*, destination_root: Path) -> tuple[str, ...]:
    launch_root = destination_root / "launch"
    launch_root.mkdir(parents=True, exist_ok=True)

    powershell_path = launch_root / "start_demo_shell.ps1"
    powershell_path.write_text(
        """param(
    [string]$Host = "127.0.0.1",
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$python = Get-Command python -ErrorAction SilentlyContinue
if ($null -eq $python) {
    throw "python was not found on PATH. Install Python 3.11+ and rerun."
}
& $python.Source (Join-Path $root "tools\\rebuild_demo_shell.py") --host $Host --port $Port
""",
        encoding="utf-8",
    )

    shell_path = launch_root / "start_demo_shell.sh"
    shell_path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
HOST="${1:-127.0.0.1}"
PORT="${2:-8000}"

python "${ROOT_DIR}/tools/rebuild_demo_shell.py" --host "${HOST}" --port "${PORT}"
""",
        encoding="utf-8",
    )

    bootstrap_powershell_path = launch_root / "bootstrap_windows_pilot.ps1"
    shutil.copy2(
        REPO_ROOT / "scripts" / "bootstrap_windows_customer_pilot.ps1",
        bootstrap_powershell_path,
    )
    readiness_powershell_path = launch_root / "check_pilot_readiness.ps1"
    shutil.copy2(
        REPO_ROOT / "scripts" / "check_customer_pilot_readiness.ps1",
        readiness_powershell_path,
    )
    return (
        "launch/start_demo_shell.ps1",
        "launch/start_demo_shell.sh",
        "launch/bootstrap_windows_pilot.ps1",
        "launch/check_pilot_readiness.ps1",
        "tools/rebuild_demo_shell.py",
        "tools/bootstrap_windows_pilot.py",
        "tools/check_pilot_readiness.py",
        "tools/verify_handoff_signature.py",
    )


def _run_manifest_pipeline(*, manifest_path: Path, base_dir: Path, state_db: Path, repo_root: Path) -> str:
    env = os.environ.copy()
    env["PYTHONPATH"] = _build_pythonpath(repo_root)
    command = [
        sys.executable,
        "-m",
        "etl_identity_engine.cli",
        "run-all",
        "--base-dir",
        str(base_dir),
        "--manifest",
        str(manifest_path),
        "--state-db",
        str(state_db),
    ]
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = "\n".join(part for part in (completed.stdout.strip(), completed.stderr.strip()) if part)
        raise RuntimeError(f"customer pilot pipeline run failed ({completed.returncode})\n{detail}")

    store = _pipeline_state_store(state_db)
    try:
        run_id = store.latest_completed_run_id()
    finally:
        store.engine.dispose()
    if run_id is None:
        raise RuntimeError("customer pilot packaging did not produce a completed persisted run")
    return run_id


def _stage_seed_dataset(*, source_manifest: Path, destination_root: Path) -> Path:
    shutil.copytree(source_manifest.parent, destination_root)
    return destination_root / source_manifest.name


def _artifact_names(
    staging_root: Path,
    *,
    extra_paths: Sequence[str] = (),
    exclude_names: Sequence[str] = (MANIFEST_NAME,),
) -> tuple[str, ...]:
    extras = set(extra_paths)
    excluded = set(exclude_names)
    return tuple(
        sorted(
            str(path.relative_to(staging_root)).replace("\\", "/")
            for path in staging_root.rglob("*")
            if path.is_file() and path.name not in excluded
        )
        + sorted(extras)
    )


def package_customer_pilot_bundle(
    *,
    output_dir: Path,
    source_manifest: Path,
    pilot_name: str,
    version: str,
    repo_root: Path = REPO_ROOT,
    generated_at_utc: str | None = None,
    source_commit: str | None = None,
    signing_key: Path | None = None,
    signer_identity: str | None = None,
    key_id: str | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    resolved_pilot_name = sanitize_pilot_name(pilot_name)
    bundle_path = output_dir / build_bundle_name(version, resolved_pilot_name)
    resolved_generated_at_utc = resolve_generated_at_utc(
        repo_root=repo_root,
        explicit_value=generated_at_utc,
    )
    zip_timestamp = _zip_entry_timestamp(resolved_generated_at_utc)

    with tempfile.TemporaryDirectory(prefix="etl-customer-pilot-") as temp_dir:
        staging_root = Path(temp_dir) / "customer_pilot"
        staging_root.mkdir(parents=True, exist_ok=True)

        staged_manifest = _stage_seed_dataset(
            source_manifest=source_manifest.resolve(),
            destination_root=staging_root / "seed_dataset",
        )
        seed_run_dir = staging_root / "seed_run"
        state_db = staging_root / "state" / "pipeline_state.sqlite"
        source_run_id = _run_manifest_pipeline(
            manifest_path=staged_manifest,
            base_dir=seed_run_dir,
            state_db=state_db,
            repo_root=repo_root,
        )

        _prepare_public_safety_demo_shell(
            state_db=state_db,
            run_id=source_run_id,
            output_dir=staging_root / "demo_shell",
        )
        _close_django_connections()
        _write_runtime_payload(destination_root=staging_root)
        _write_rebuild_tool(destination_root=staging_root)
        _write_windows_bootstrap_tool(destination_root=staging_root)
        _write_readiness_tool(destination_root=staging_root)
        _write_signature_tool(destination_root=staging_root)
        launch_helpers = _write_launch_helpers(destination_root=staging_root)
        _write_pilot_readme(
            destination=staging_root / "README.md",
            pilot_name=resolved_pilot_name,
            version=version,
            source_run_id=source_run_id,
        )

        artifact_names = _artifact_names(
            staging_root,
            extra_paths=(
                HANDOFF_MANIFEST_NAME,
                *( (HANDOFF_SIGNATURE_NAME,) if signing_key is not None else () ),
            ),
        )
        manifest = build_manifest(
            version=version,
            pilot_name=resolved_pilot_name,
            generated_at_utc=resolved_generated_at_utc,
            source_commit=source_commit or resolve_source_commit(repo_root),
            source_manifest="seed_dataset/" + staged_manifest.name,
            source_run_id=source_run_id,
            state_db="state/pipeline_state.sqlite",
            demo_shell_dir="demo_shell",
            launch_helpers=launch_helpers,
            artifacts=artifact_names,
        )
        manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
        (staging_root / MANIFEST_NAME).write_bytes(manifest_bytes)
        handoff_manifest = build_handoff_manifest(
            version=version,
            pilot_name=resolved_pilot_name,
            generated_at_utc=resolved_generated_at_utc,
            source_commit=source_commit or resolve_source_commit(repo_root),
            source_manifest="seed_dataset/" + staged_manifest.name,
            source_run_id=source_run_id,
            verification_type="sha256",
            artifacts=_artifact_hash_entries(
                staging_root,
                exclude_names=(HANDOFF_MANIFEST_NAME,),
            ),
        )
        handoff_manifest_bytes = (
            json.dumps(handoff_manifest, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        (staging_root / HANDOFF_MANIFEST_NAME).write_bytes(handoff_manifest_bytes)
        if signing_key is not None:
            _write_detached_signature(
                destination=staging_root / HANDOFF_SIGNATURE_NAME,
                manifest_path=HANDOFF_MANIFEST_NAME,
                manifest_bytes=handoff_manifest_bytes,
                private_key_path=signing_key,
                signer_identity=signer_identity,
                key_id=key_id,
            )

        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for source_path in sorted(staging_root.rglob("*")):
                if not source_path.is_file():
                    continue
                relative_path = source_path.relative_to(staging_root).as_posix()
                zip_info = zipfile.ZipInfo(relative_path, date_time=zip_timestamp)
                zip_info.compress_type = zipfile.ZIP_DEFLATED
                zip_info.external_attr = 0o100644 << 16
                archive.writestr(zip_info, source_path.read_bytes())

    return bundle_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    source_manifest = Path(args.manifest)
    pilot_name = args.pilot_name or source_manifest.resolve().parent.name
    version = args.version or read_project_version()
    bundle_path = package_customer_pilot_bundle(
        output_dir=resolve_output_dir(args.output_dir),
        source_manifest=source_manifest,
        pilot_name=pilot_name,
        version=version,
        signing_key=None if args.signing_key is None else Path(args.signing_key).resolve(),
        signer_identity=args.signer_identity,
        key_id=args.key_id,
    )
    print(f"customer pilot bundle written: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
