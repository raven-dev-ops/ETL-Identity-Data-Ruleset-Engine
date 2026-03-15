from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
from typing import Sequence
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
PILOT_MANIFEST_NAME = "pilot_manifest.json"
HANDOFF_MANIFEST_NAME = "pilot_handoff_manifest.json"
DEFAULT_MIN_FREE_GIB = 2.0
DEFAULT_DEMO_PORT = 8000
DEFAULT_SERVICE_PORT = 8010
REQUIRED_BUNDLE_PATHS = (
    PILOT_MANIFEST_NAME,
    HANDOFF_MANIFEST_NAME,
    "runtime/requirements-pilot.txt",
    "runtime/config/runtime_environments.yml",
    "launch/bootstrap_windows_pilot.ps1",
    "launch/check_pilot_readiness.ps1",
    "tools/bootstrap_windows_pilot.py",
    "tools/check_pilot_readiness.py",
    "state/pipeline_state.sqlite",
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate that a customer pilot bundle and target Windows host "
            "meet the documented single-host pilot baseline."
        )
    )
    parser.add_argument("--bundle", default=None, help="Path to a packaged customer pilot zip.")
    parser.add_argument(
        "--bundle-root",
        default=None,
        help="Path to an extracted customer pilot bundle root.",
    )
    parser.add_argument(
        "--install-root",
        default=None,
        help="Target extraction/install root to validate for free space and readiness.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON output path for the readiness summary.",
    )
    parser.add_argument("--demo-port", default=DEFAULT_DEMO_PORT, type=int)
    parser.add_argument("--service-port", default=DEFAULT_SERVICE_PORT, type=int)
    parser.add_argument("--min-free-gib", default=DEFAULT_MIN_FREE_GIB, type=float)
    return parser.parse_args(argv)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _load_json_bytes(payload: bytes) -> dict[str, object]:
    return json.loads(payload.decode("utf-8"))


def _resolve_bundle_and_root(
    *,
    bundle: str | None,
    bundle_root: str | None,
) -> tuple[Path | None, Path | None]:
    if bundle and bundle_root:
        raise ValueError("--bundle and --bundle-root are mutually exclusive")
    if bundle:
        resolved_bundle = Path(bundle).resolve()
        if not resolved_bundle.exists():
            raise FileNotFoundError(f"Customer pilot bundle not found: {resolved_bundle}")
        return resolved_bundle, None
    if bundle_root:
        resolved_root = Path(bundle_root).resolve()
        if not resolved_root.exists():
            raise FileNotFoundError(f"Customer pilot bundle root not found: {resolved_root}")
        return None, resolved_root
    raise ValueError("Provide either --bundle or --bundle-root")


def _default_install_root(*, bundle_path: Path | None, bundle_root: Path | None) -> Path:
    if bundle_root is not None:
        return bundle_root
    assert bundle_path is not None
    return bundle_path.with_suffix("")


def _nearest_existing_parent(path: Path) -> Path:
    current = path if path.exists() else path.parent
    while not current.exists():
        current = current.parent
    return current


def _path_writable(path: Path) -> bool:
    try:
        probe = path / ".readiness-write-test"
        probe.write_text("ok\n", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


def _port_is_available(port: int, *, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _docker_server_ready() -> bool:
    if not _docker_available():
        return False
    completed = subprocess.run(
        ["docker", "info"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return completed.returncode == 0


def _verify_handoff_manifest_payload(
    *,
    handoff_manifest: dict[str, object],
    artifact_lookup,
) -> tuple[list[dict[str, object]], list[str]]:
    checks: list[dict[str, object]] = []
    errors: list[str] = []
    required_keys = {
        "project",
        "bundle_type",
        "version",
        "pilot_name",
        "generated_at_utc",
        "source_commit",
        "source_manifest",
        "source_run_id",
        "verification_type",
        "artifacts",
    }
    manifest_keys = set(handoff_manifest)
    checks.append(
        {
            "check": "handoff_manifest_keys",
            "status": "ok" if required_keys <= manifest_keys else "error",
            "detail": sorted(manifest_keys),
        }
    )
    if not required_keys <= manifest_keys:
        errors.append("pilot_handoff_manifest.json is missing required metadata keys")

    artifacts = handoff_manifest.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        checks.append({"check": "handoff_manifest_artifacts", "status": "error", "detail": "missing"})
        errors.append("pilot_handoff_manifest.json must contain artifact hash entries")
        return checks, errors

    for entry in artifacts:
        if not isinstance(entry, dict):
            errors.append("pilot_handoff_manifest.json contains a malformed artifact entry")
            continue
        relative_path = str(entry.get("path", "")).strip()
        expected_sha256 = str(entry.get("sha256", "")).strip()
        expected_size = entry.get("size_bytes")
        try:
            payload = artifact_lookup(relative_path)
        except FileNotFoundError:
            checks.append(
                {
                    "check": f"artifact:{relative_path}",
                    "status": "error",
                    "detail": "missing",
                }
            )
            errors.append(f"handoff manifest artifact is missing: {relative_path}")
            continue
        actual_sha256 = _sha256_bytes(payload)
        actual_size = len(payload)
        is_ok = (
            bool(relative_path)
            and bool(expected_sha256)
            and expected_sha256 == actual_sha256
            and expected_size == actual_size
        )
        checks.append(
            {
                "check": f"artifact:{relative_path}",
                "status": "ok" if is_ok else "error",
                "detail": {
                    "expected_sha256": expected_sha256,
                    "actual_sha256": actual_sha256,
                    "expected_size": expected_size,
                    "actual_size": actual_size,
                },
            }
        )
        if not is_ok:
            errors.append(f"handoff manifest verification failed for {relative_path}")
    return checks, errors


def _inspect_bundle_zip(bundle_path: Path) -> tuple[dict[str, object], list[dict[str, object]], list[str]]:
    checks: list[dict[str, object]] = []
    errors: list[str] = []
    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        missing_required = [path for path in REQUIRED_BUNDLE_PATHS if path not in members]
        checks.append(
            {
                "check": "bundle_required_paths",
                "status": "ok" if not missing_required else "error",
                "detail": sorted(members & set(REQUIRED_BUNDLE_PATHS)),
            }
        )
        if missing_required:
            errors.append(
                "customer pilot bundle is missing required paths: " + ", ".join(missing_required)
            )
        handoff_manifest = _load_json_bytes(archive.read(HANDOFF_MANIFEST_NAME))

        def artifact_lookup(relative_path: str) -> bytes:
            if relative_path not in members:
                raise FileNotFoundError(relative_path)
            return archive.read(relative_path)

        handoff_checks, handoff_errors = _verify_handoff_manifest_payload(
            handoff_manifest=handoff_manifest,
            artifact_lookup=artifact_lookup,
        )
        checks.extend(handoff_checks)
        errors.extend(handoff_errors)
    return handoff_manifest, checks, errors


def _inspect_bundle_root(bundle_root: Path) -> tuple[dict[str, object], list[dict[str, object]], list[str]]:
    checks: list[dict[str, object]] = []
    errors: list[str] = []
    missing_required = [
        path for path in REQUIRED_BUNDLE_PATHS if not (bundle_root / path.replace("/", os.sep)).exists()
    ]
    checks.append(
        {
            "check": "bundle_required_paths",
            "status": "ok" if not missing_required else "error",
            "detail": sorted(set(REQUIRED_BUNDLE_PATHS) - set(missing_required)),
        }
    )
    if missing_required:
        errors.append(
            "customer pilot bundle root is missing required paths: " + ", ".join(missing_required)
        )
    handoff_manifest = _load_json_bytes((bundle_root / HANDOFF_MANIFEST_NAME).read_bytes())

    def artifact_lookup(relative_path: str) -> bytes:
        target = bundle_root / relative_path.replace("/", os.sep)
        if not target.exists():
            raise FileNotFoundError(relative_path)
        return target.read_bytes()

    handoff_checks, handoff_errors = _verify_handoff_manifest_payload(
        handoff_manifest=handoff_manifest,
        artifact_lookup=artifact_lookup,
    )
    checks.extend(handoff_checks)
    errors.extend(handoff_errors)
    return handoff_manifest, checks, errors


def evaluate_customer_pilot_readiness(
    *,
    bundle: str | None = None,
    bundle_root: str | None = None,
    install_root: str | None = None,
    demo_port: int = DEFAULT_DEMO_PORT,
    service_port: int = DEFAULT_SERVICE_PORT,
    min_free_gib: float = DEFAULT_MIN_FREE_GIB,
    python_version: Sequence[int] | None = None,
    system_name: str | None = None,
    docker_available: bool | None = None,
    docker_server_ready: bool | None = None,
    free_bytes: int | None = None,
) -> dict[str, object]:
    resolved_bundle_path, resolved_bundle_root = _resolve_bundle_and_root(
        bundle=bundle,
        bundle_root=bundle_root,
    )
    resolved_install_root = (
        Path(install_root).resolve()
        if install_root is not None
        else _default_install_root(bundle_path=resolved_bundle_path, bundle_root=resolved_bundle_root)
    )

    checks: list[dict[str, object]] = []
    errors: list[str] = []
    warnings: list[str] = []

    if resolved_bundle_path is not None:
        handoff_manifest, bundle_checks, bundle_errors = _inspect_bundle_zip(resolved_bundle_path)
    else:
        assert resolved_bundle_root is not None
        handoff_manifest, bundle_checks, bundle_errors = _inspect_bundle_root(resolved_bundle_root)
    checks.extend(bundle_checks)
    errors.extend(bundle_errors)

    resolved_system_name = system_name or platform.system()
    checks.append(
        {
            "check": "host_platform",
            "status": "ok" if resolved_system_name == "Windows" else "error",
            "detail": resolved_system_name,
        }
    )
    if resolved_system_name != "Windows":
        errors.append("customer pilot baseline is supported on Windows hosts")

    resolved_python_version = tuple(python_version or sys.version_info[:3])
    checks.append(
        {
            "check": "python_version",
            "status": "ok" if resolved_python_version >= (3, 11, 0) else "error",
            "detail": ".".join(str(part) for part in resolved_python_version[:3]),
        }
    )
    if resolved_python_version < (3, 11, 0):
        errors.append("customer pilot baseline requires Python 3.11 or newer")

    has_docker = _docker_available() if docker_available is None else docker_available
    checks.append(
        {
            "check": "docker_cli",
            "status": "ok" if has_docker else "error",
            "detail": "available" if has_docker else "missing",
        }
    )
    if not has_docker:
        errors.append("Docker Desktop CLI is required for the supported bootstrap baseline")

    docker_ready = _docker_server_ready() if docker_server_ready is None else docker_server_ready
    checks.append(
        {
            "check": "docker_daemon",
            "status": "ok" if docker_ready else "error",
            "detail": "reachable" if docker_ready else "unreachable",
        }
    )
    if not docker_ready:
        errors.append("Docker Desktop must be running for the supported bootstrap baseline")

    if resolved_bundle_root is None:
        target_parent = _nearest_existing_parent(resolved_install_root)
        install_ready = not resolved_install_root.exists() or not any(resolved_install_root.iterdir())
        writable = _path_writable(target_parent)
        checks.append(
            {
                "check": "install_root_state",
                "status": "ok" if install_ready and writable else "error",
                "detail": str(resolved_install_root),
            }
        )
        if not install_ready:
            errors.append("install root must not already exist with content for bundle extraction")
        if not writable:
            errors.append("install root parent is not writable")
    else:
        checks.append(
            {
                "check": "install_root_state",
                "status": "ok",
                "detail": str(resolved_install_root),
            }
        )
        target_parent = _nearest_existing_parent(resolved_install_root)

    resolved_free_bytes = (
        shutil.disk_usage(target_parent).free if free_bytes is None else int(free_bytes)
    )
    min_free_bytes = int(min_free_gib * (1024**3))
    checks.append(
        {
            "check": "disk_space",
            "status": "ok" if resolved_free_bytes >= min_free_bytes else "error",
            "detail": {
                "free_bytes": resolved_free_bytes,
                "required_bytes": min_free_bytes,
            },
        }
    )
    if resolved_free_bytes < min_free_bytes:
        errors.append("insufficient free disk space for the documented pilot baseline")

    for label, port in (("demo_port", demo_port), ("service_port", service_port)):
        available = _port_is_available(port)
        checks.append(
            {
                "check": label,
                "status": "ok" if available else "warning",
                "detail": port,
            }
        )
        if not available:
            warnings.append(f"default {label.replace('_', ' ')} {port} is already in use")

    return {
        "status": "error" if errors else ("warning" if warnings else "ok"),
        "bundle": None if resolved_bundle_path is None else str(resolved_bundle_path),
        "bundle_root": None if resolved_bundle_root is None else str(resolved_bundle_root),
        "install_root": str(resolved_install_root),
        "pilot_name": handoff_manifest.get("pilot_name"),
        "version": handoff_manifest.get("version"),
        "verification_type": handoff_manifest.get("verification_type"),
        "checks": checks,
        "warnings": warnings,
        "errors": errors,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = evaluate_customer_pilot_readiness(
        bundle=args.bundle,
        bundle_root=args.bundle_root,
        install_root=args.install_root,
        demo_port=args.demo_port,
        service_port=args.service_port,
        min_free_gib=args.min_free_gib,
    )
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not summary["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
