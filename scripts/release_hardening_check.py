"""Build release artifacts and emit dependency-inventory plus audit outputs."""

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
DEFAULT_OUTPUT_DIR = REPO_ROOT / "dist" / "release-hardening"
PROJECT_NAME = "etl-identity-engine"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build release artifacts and emit dependency-inventory plus audit outputs."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory that will receive built artifacts plus hardening reports.",
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


def _read_project_version(pyproject_path: Path = REPO_ROOT / "pyproject.toml") -> str:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    version = str(pyproject["project"]["version"]).strip()
    if not version:
        raise SystemExit(f"Missing project.version in {pyproject_path}")
    return version


def _build_artifacts(python_executable: str, artifact_dir: Path) -> list[Path]:
    if artifact_dir.exists():
        shutil.rmtree(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    repo_build_dir = REPO_ROOT / "build"
    build_dir_existed = repo_build_dir.exists()
    try:
        completed = _run_command(
            [
                python_executable,
                "-m",
                "build",
                "--sdist",
                "--wheel",
                "--outdir",
                str(artifact_dir),
            ]
        )
        if completed.returncode != 0:
            raise SystemExit(completed.returncode)
    finally:
        if not build_dir_existed and repo_build_dir.exists():
            shutil.rmtree(repo_build_dir)

    built_paths = sorted(path for path in artifact_dir.iterdir() if path.is_file())
    if len([path for path in built_paths if path.suffix == ".whl"]) != 1 or len(
        [path for path in built_paths if path.suffixes[-2:] == [".tar", ".gz"]]
    ) != 1:
        raise SystemExit("Release hardening build did not produce exactly one wheel and one sdist artifact.")
    return built_paths


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_inventory(paths: list[Path], *, output_root: Path) -> list[dict[str, object]]:
    return [
        {
            "name": path.name,
            "relative_path": str(path.relative_to(output_root)),
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }
        for path in paths
    ]


def _capture_json(command: list[str], *, failure_hint: str, env: dict[str, str] | None = None) -> Any:
    completed = _run_command(command, capture_output=True, env=env)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise SystemExit(f"{failure_hint}: {detail}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{failure_hint}: command did not return valid JSON") from exc


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run_dependency_audit(python_executable: str, output_path: Path) -> None:
    utf8_env = os.environ.copy()
    utf8_env["PYTHONIOENCODING"] = "utf-8"
    utf8_env["PYTHONUTF8"] = "1"
    completed = _run_command(
        [
            python_executable,
            "-m",
            "pip_audit",
            "--local",
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
        raise SystemExit(f"Dependency audit failed unexpectedly: {detail}")
    try:
        audit_payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit("Dependency audit did not return valid JSON output") from exc

    _write_json(output_path, audit_payload)
    if completed.returncode == 1:
        raise SystemExit(
            f"Dependency audit found one or more vulnerabilities. See {output_path.relative_to(REPO_ROOT)}."
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    python_executable = sys.executable
    output_dir = Path(args.output_dir).resolve()
    artifact_dir = output_dir / "artifacts"
    dependency_inventory_path = output_dir / "dependency_inventory.json"
    dependency_audit_path = output_dir / "dependency_audit.json"
    summary_path = output_dir / "release_hardening_summary.json"

    output_dir.mkdir(parents=True, exist_ok=True)
    built_artifacts = _build_artifacts(python_executable, artifact_dir)
    utf8_env = os.environ.copy()
    utf8_env["PYTHONIOENCODING"] = "utf-8"
    utf8_env["PYTHONUTF8"] = "1"
    dependency_inventory = _capture_json(
        [python_executable, "-X", "utf8", "-m", "pip", "inspect", "--local"],
        failure_hint="Dependency inventory generation failed",
        env=utf8_env,
    )
    _write_json(dependency_inventory_path, dependency_inventory)
    _run_dependency_audit(python_executable, dependency_audit_path)

    summary = {
        "project": PROJECT_NAME,
        "version": _read_project_version(),
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "artifacts": _artifact_inventory(built_artifacts, output_root=output_dir),
        "dependency_inventory_path": str(dependency_inventory_path.relative_to(output_dir)),
        "dependency_audit_path": str(dependency_audit_path.relative_to(output_dir)),
        "inspected_package_count": len(dependency_inventory.get("installed", [])),
    }
    _write_json(summary_path, summary)
    print(f"release hardening outputs written: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
