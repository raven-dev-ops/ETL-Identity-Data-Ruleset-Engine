"""Cross-platform local validation runner."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPO = "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine"


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


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    python_executable = sys.executable

    _run_command((python_executable, "-m", "ruff", "check", "."))
    _run_command((python_executable, "-m", "pytest"))
    _run_command(
        (
            python_executable,
            "scripts/create_github_backlog.py",
            "--repo",
            args.repo,
            "--dry-run",
        )
    )

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
