"""Cross-platform local validation runner."""

from __future__ import annotations

import argparse
import os
from importlib import metadata
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPO = "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine"
PROJECT_DISTRIBUTION = "etl-identity-engine"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the documented local validation path."
    )
    parser.add_argument(
        "--repo",
        default=DEFAULT_REPO,
        help="Repository slug used for GitHub-backed dry-run and optional remote checks.",
    )
    parser.add_argument(
        "--include-remote-github-checks",
        action="store_true",
        help="Also run the deployed-state GitHub metadata verification path.",
    )
    return parser.parse_args(argv)


def _run_command(command: Sequence[str], *, env: dict[str, str] | None = None) -> None:
    completed = subprocess.run(
        list(command),
        cwd=REPO_ROOT,
        env=env,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def _capture_output(command: Sequence[str]) -> str:
    completed = subprocess.run(
        list(command),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise SystemExit(
            f"command failed ({completed.returncode}): {' '.join(command)}\n{detail}"
        )
    return completed.stdout.strip()


def _resolve_gh_executable() -> str:
    candidates = (
        REPO_ROOT / ".venv" / "Scripts" / "gh.exe",
        REPO_ROOT / ".venv" / "bin" / "gh",
    )
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    path_gh = shutil.which("gh")
    if path_gh:
        return path_gh

    raise SystemExit(
        "GitHub CLI not found. Run the bootstrap script to install the venv-scoped gh binary."
    )


def _read_project_version(pyproject_path: Path = REPO_ROOT / "pyproject.toml") -> str:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    version = str(pyproject["project"]["version"]).strip()
    if not version:
        raise SystemExit(f"Missing project.version in {pyproject_path}")
    return version


def verify_installed_distribution_version(
    *,
    pyproject_path: Path = REPO_ROOT / "pyproject.toml",
    distribution_name: str = PROJECT_DISTRIBUTION,
) -> tuple[str, str]:
    project_version = _read_project_version(pyproject_path)

    try:
        installed_version = metadata.version(distribution_name)
    except metadata.PackageNotFoundError as exc:
        raise SystemExit(
            "The project is not installed in the active environment. "
            "Run the bootstrap script or `python -m pip install -e .[dev]` first."
        ) from exc

    if installed_version != project_version:
        raise SystemExit(
            "Installed distribution metadata is out of date: "
            f"{distribution_name}=={installed_version} but pyproject.toml declares {project_version}. "
            "Re-run the bootstrap script or `python -m pip install -e .[dev]`."
        )

    return project_version, installed_version


def verify_distribution_build(
    python_executable: str,
    *,
    temp_root: str | None = None,
) -> tuple[str, str]:
    repo_build_dir = REPO_ROOT / "build"
    build_dir_existed = repo_build_dir.exists()

    with tempfile.TemporaryDirectory(
        prefix="etl-identity-engine-build-",
        dir=temp_root,
    ) as temp_dir:
        output_dir = Path(temp_dir)

        try:
            _run_command(
                (
                    python_executable,
                    "-m",
                    "build",
                    "--sdist",
                    "--wheel",
                    "--outdir",
                    str(output_dir),
                )
            )
        finally:
            if not build_dir_existed and repo_build_dir.exists():
                shutil.rmtree(repo_build_dir)

        wheel_names = sorted(path.name for path in output_dir.glob("*.whl"))
        sdist_names = sorted(path.name for path in output_dir.glob("*.tar.gz"))

        if len(wheel_names) != 1 or len(sdist_names) != 1:
            raise SystemExit(
                "Distribution build did not produce exactly one wheel and one sdist artifact."
            )

        return sdist_names[0], wheel_names[0]


def verify_console_entrypoint(python_executable: str) -> Path:
    scripts_dir = Path(python_executable).resolve().parent
    candidate_names: list[str] = [PROJECT_DISTRIBUTION, PROJECT_DISTRIBUTION.replace("-", "_")]
    try:
        distribution = metadata.distribution(PROJECT_DISTRIBUTION)
    except metadata.PackageNotFoundError:
        distribution = None
    if distribution is not None:
        for entry_point in distribution.entry_points:
            if entry_point.group == "console_scripts":
                candidate_names.append(entry_point.name)

    normalized_candidates: list[str] = []
    for candidate_name in candidate_names:
        if candidate_name not in normalized_candidates:
            normalized_candidates.append(candidate_name)

    candidate_names_with_suffixes: list[str] = []
    for candidate_name in normalized_candidates:
        candidate_names_with_suffixes.append(candidate_name)
        candidate_names_with_suffixes.append(f"{candidate_name}.exe")
        candidate_names_with_suffixes.append(f"{candidate_name}.cmd")

    for candidate_name in candidate_names_with_suffixes:
        candidate_path = scripts_dir / candidate_name
        if not candidate_path.exists():
            continue

        help_text = _capture_output((str(candidate_path), "--help"))
        if "run-all" not in help_text or "generate" not in help_text:
            raise SystemExit(
                f"Console entrypoint {candidate_path} did not expose the expected CLI help output."
            )
        return candidate_path

    python_path = Path(python_executable)
    if python_path.exists():
        help_text = _capture_output((python_executable, "-m", "etl_identity_engine.cli", "--help"))
        if "run-all" not in help_text or "generate" not in help_text:
            raise SystemExit(
                "The installed CLI module did not expose the expected help output when run via "
                f"`{python_executable} -m etl_identity_engine.cli --help`."
            )
        return python_path

    raise SystemExit(
        f"Console entrypoint {PROJECT_DISTRIBUTION!r} was not found next to {python_executable}. "
        "Re-run the bootstrap script or `python -m pip install -e .[dev]`."
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    python_executable = sys.executable

    verify_installed_distribution_version()
    verify_distribution_build(python_executable)
    verify_console_entrypoint(python_executable)
    _run_command((python_executable, "-m", "ruff", "check", "."))
    _run_command((python_executable, "-m", "pytest"))

    with tempfile.TemporaryDirectory(prefix="etl-identity-engine-release-sample-") as temp_dir:
        _run_command(
            (
                python_executable,
                "scripts/package_release_sample.py",
                "--output-dir",
                temp_dir,
                "--profile",
                "small",
                "--seed",
                "42",
                "--formats",
                "csv,parquet",
            )
        )

    _run_command((python_executable, "scripts/persisted_state_recovery_smoke.py"))

    if not args.include_remote_github_checks:
        return 0

    gh_executable = _resolve_gh_executable()
    gh_token = _capture_output((gh_executable, "auth", "token"))
    if not gh_token:
        raise SystemExit(
            "Unable to read a GitHub token from the gh CLI. Run gh auth login first."
        )

    env = os.environ.copy()
    env["GH_TOKEN"] = gh_token
    _run_command(
        (
            python_executable,
            "scripts/verify_github_issue_metadata.py",
            "--repo",
            args.repo,
        ),
        env=env,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
