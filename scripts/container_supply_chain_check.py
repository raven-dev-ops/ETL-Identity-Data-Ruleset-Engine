"""Build, attest, and scan the container image release artifact."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "dist" / "container-supply-chain"
DEFAULT_IMAGE_TAG = "etl-identity-engine:release-hardening"
PROJECT_NAME = "etl-identity-engine"
ATTESTATION_PREDICATE_TYPE = (
    "https://raven-dev-ops.github.io/ETL-Identity-Data-Ruleset-Engine/container-attestation/v1"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the container image and emit attestation plus scan outputs."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory that will receive attestation, inventory, and scan outputs.",
    )
    parser.add_argument(
        "--image-tag",
        default=DEFAULT_IMAGE_TAG,
        help="Container image tag to build and inspect.",
    )
    return parser.parse_args(argv)


def _run_command(
    command: list[str],
    *,
    capture_output: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=capture_output,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        check=False,
    )


def _capture_json(command: list[str], *, failure_hint: str, env: dict[str, str] | None = None) -> Any:
    completed = _run_command(command, capture_output=True, env=env)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise SystemExit(f"{failure_hint}: {detail}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{failure_hint}: command did not return valid JSON") from exc


def _capture_text(command: list[str], *, failure_hint: str, env: dict[str, str] | None = None) -> str:
    completed = _run_command(command, capture_output=True, env=env)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise SystemExit(f"{failure_hint}: {detail}")
    return completed.stdout


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_project_version(pyproject_path: Path = REPO_ROOT / "pyproject.toml") -> str:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    version = str(pyproject["project"]["version"]).strip()
    if not version:
        raise SystemExit(f"Missing project.version in {pyproject_path}")
    return version


def _git_head_commit() -> str:
    completed = _run_command(["git", "rev-parse", "HEAD"], capture_output=True)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise SystemExit(f"Unable to resolve git HEAD: {detail}")
    return completed.stdout.strip()


def _resolve_pip_audit_executable() -> str:
    candidates = (
        Path(sys.executable).resolve().parent / "pip-audit.exe",
        Path(sys.executable).resolve().parent / "pip-audit",
    )
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    path_candidate = shutil.which("pip-audit")
    if path_candidate:
        return path_candidate

    raise SystemExit(
        "pip-audit executable not found in the active environment. "
        "Run `python -m pip install -e .[dev]` before the container supply-chain check."
    )


def _normalize_image_inspect(inspect_payload: Any, *, image_tag: str) -> dict[str, object]:
    if not isinstance(inspect_payload, list) or len(inspect_payload) != 1 or not isinstance(
        inspect_payload[0], dict
    ):
        raise SystemExit("docker image inspect did not return the expected single-image payload")
    payload = inspect_payload[0]
    rootfs = payload.get("RootFS")
    config = payload.get("Config")
    return {
        "image_tag": image_tag,
        "image_id": str(payload.get("Id", "")),
        "repo_tags": list(payload.get("RepoTags") or []),
        "repo_digests": list(payload.get("RepoDigests") or []),
        "created": str(payload.get("Created", "")),
        "architecture": str(payload.get("Architecture", "")),
        "os": str(payload.get("Os", "")),
        "rootfs_layers": list(rootfs.get("Layers") if isinstance(rootfs, dict) else []),
        "labels": dict(config.get("Labels") if isinstance(config, dict) and isinstance(config.get("Labels"), dict) else {}),
    }


def _extract_python_packages(pip_inspect_payload: Any) -> list[dict[str, str]]:
    if not isinstance(pip_inspect_payload, dict):
        raise SystemExit("pip inspect did not return a mapping payload")
    installed = pip_inspect_payload.get("installed")
    if not isinstance(installed, list):
        raise SystemExit("pip inspect payload does not contain an installed package list")

    packages: list[dict[str, str]] = []
    for item in installed:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, dict):
            continue
        name = str(metadata.get("name", "")).strip()
        version = str(metadata.get("version", "")).strip()
        if name and version:
            packages.append({"name": name, "version": version})
    packages.sort(key=lambda item: item["name"].lower())
    return packages


def _parse_dpkg_query_output(output: str) -> list[dict[str, str]]:
    packages: list[dict[str, str]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        name, separator, version = line.partition("\t")
        if not separator:
            continue
        normalized_name = name.strip()
        normalized_version = version.strip()
        if normalized_name and normalized_version:
            packages.append({"name": normalized_name, "version": normalized_version})
    packages.sort(key=lambda item: item["name"].lower())
    return packages


def _write_requirements_lock(
    path: Path,
    packages: list[dict[str, str]],
    *,
    excluded_package_names: set[str] | None = None,
) -> None:
    excluded = {name.lower() for name in (excluded_package_names or set())}
    lines = [
        f"{package['name']}=={package['version']}"
        for package in packages
        if package["name"].lower() not in excluded
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _artifact_inventory(paths: list[Path], *, output_root: Path) -> list[dict[str, object]]:
    return [
        {
            "name": path.name,
            "relative_path": str(path.relative_to(output_root)),
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }
        for path in sorted(paths)
    ]


def _build_attestation(
    *,
    image: dict[str, object],
    output_root: Path,
    artifact_paths: list[Path],
    generated_at_utc: str,
    source_commit: str,
) -> dict[str, object]:
    return {
        "project": PROJECT_NAME,
        "predicate_type": ATTESTATION_PREDICATE_TYPE,
        "generated_at_utc": generated_at_utc,
        "source_commit": source_commit,
        "subject": {
            "image_tag": image["image_tag"],
            "image_id": image["image_id"],
            "repo_digests": image["repo_digests"],
        },
        "artifacts": _artifact_inventory(artifact_paths, output_root=output_root),
    }


def _run_dependency_audit(requirements_lock_path: Path, output_path: Path) -> None:
    pip_audit_executable = _resolve_pip_audit_executable()
    utf8_env = os.environ.copy()
    utf8_env["PYTHONIOENCODING"] = "utf-8"
    utf8_env["PYTHONUTF8"] = "1"
    completed = _run_command(
        [
            pip_audit_executable,
            "--requirement",
            str(requirements_lock_path),
            "--format",
            "json",
            "--progress-spinner",
            "off",
        ],
        capture_output=True,
        env=utf8_env,
    )
    if completed.returncode not in (0, 1):
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise SystemExit(f"Container dependency audit failed unexpectedly: {detail}")
    try:
        audit_payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit("Container dependency audit did not return valid JSON output") from exc

    _write_json(output_path, audit_payload)
    if completed.returncode == 1:
        raise SystemExit(
            "Container dependency audit found one or more vulnerabilities. "
            f"See {output_path.relative_to(REPO_ROOT)}."
        )


def _build_image(image_tag: str) -> None:
    completed = _run_command(["docker", "build", "-t", image_tag, "."])
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    source_commit = _git_head_commit()

    _build_image(args.image_tag)

    image_inspect = _capture_json(
        ["docker", "image", "inspect", args.image_tag],
        failure_hint="Container image inspection failed",
    )
    normalized_image = _normalize_image_inspect(image_inspect, image_tag=args.image_tag)

    pip_inspect = _capture_json(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "python",
            args.image_tag,
            "-m",
            "pip",
            "inspect",
            "--local",
        ],
        failure_hint="Container Python inventory collection failed",
    )
    python_packages = _extract_python_packages(pip_inspect)

    dpkg_output = _capture_text(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "sh",
            args.image_tag,
            "-c",
            r"dpkg-query -W -f='${Package}\t${Version}\n'",
        ],
        failure_hint="Container OS inventory collection failed",
    )
    os_packages = _parse_dpkg_query_output(dpkg_output)

    requirements_lock_path = output_dir / "container_requirements.txt"
    _write_requirements_lock(
        requirements_lock_path,
        python_packages,
        excluded_package_names={PROJECT_NAME},
    )

    sbom_path = output_dir / "container_sbom.json"
    _write_json(
        sbom_path,
        {
            "project": PROJECT_NAME,
            "generated_at_utc": generated_at_utc,
            "image": normalized_image,
            "python_packages": python_packages,
            "os_packages": os_packages,
        },
    )

    provenance_path = output_dir / "container_provenance.json"
    _write_json(
        provenance_path,
        {
            "project": PROJECT_NAME,
            "version": _read_project_version(),
            "generated_at_utc": generated_at_utc,
            "source_commit": source_commit,
            "image": normalized_image,
            "materials": [
                {"path": "Dockerfile", "sha256": _sha256(REPO_ROOT / "Dockerfile")},
                {"path": "pyproject.toml", "sha256": _sha256(REPO_ROOT / "pyproject.toml")},
                {
                    "path": "config/runtime_environments.yml",
                    "sha256": _sha256(REPO_ROOT / "config" / "runtime_environments.yml"),
                },
            ],
        },
    )

    audit_path = output_dir / "container_dependency_audit.json"
    _run_dependency_audit(requirements_lock_path, audit_path)

    attestation_path = output_dir / "container_attestation.json"
    _write_json(
        attestation_path,
        _build_attestation(
            image=normalized_image,
            output_root=output_dir,
            artifact_paths=[requirements_lock_path, sbom_path, provenance_path, audit_path],
            generated_at_utc=generated_at_utc,
            source_commit=source_commit,
        ),
    )

    summary_path = output_dir / "container_supply_chain_summary.json"
    _write_json(
        summary_path,
        {
            "project": PROJECT_NAME,
            "version": _read_project_version(),
            "generated_at_utc": generated_at_utc,
            "image_tag": args.image_tag,
            "image_id": normalized_image["image_id"],
            "container_sbom_path": str(sbom_path.relative_to(output_dir)),
            "container_provenance_path": str(provenance_path.relative_to(output_dir)),
            "container_dependency_audit_path": str(audit_path.relative_to(output_dir)),
            "container_attestation_path": str(attestation_path.relative_to(output_dir)),
            "python_package_count": len(python_packages),
            "os_package_count": len(os_packages),
        },
    )

    print(f"container supply-chain outputs written: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
