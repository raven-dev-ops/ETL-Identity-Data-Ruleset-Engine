import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_run_checks_python_entrypoint_covers_active_backlog_and_release_sample() -> None:
    python_text = (REPO_ROOT / "scripts" / "run_checks.py").read_text(encoding="utf-8")

    expected_fragments = (
        "-m",
        "build",
        "create_github_backlog.py",
        "--repo",
        "--dry-run",
        "package_release_sample.py",
        "etl-identity-engine",
        "--help",
        "TemporaryDirectory",
    )

    for fragment in expected_fragments:
        assert fragment in python_text


def test_run_checks_wrappers_delegate_to_python_entrypoint() -> None:
    powershell_text = (REPO_ROOT / "scripts" / "run_checks.ps1").read_text(encoding="utf-8")
    bash_text = (REPO_ROOT / "scripts" / "run_checks.sh").read_text(encoding="utf-8")

    assert "scripts\\run_checks.py" in powershell_text
    assert "scripts/run_checks.py" in bash_text
    assert "--include-remote-github-checks" in powershell_text
    assert "--include-remote-github-checks" in bash_text


def test_run_checks_python_entrypoint_uses_temporary_release_sample_output() -> None:
    python_text = (REPO_ROOT / "scripts" / "run_checks.py").read_text(encoding="utf-8")

    assert "TemporaryDirectory" in python_text
    assert "etl-identity-engine-release-sample-" in python_text
    assert "dist/release-samples" not in python_text
    assert "dist\\\\release-samples" not in python_text


def test_run_pipeline_wrappers_delegate_to_python_entrypoint() -> None:
    powershell_text = (REPO_ROOT / "scripts" / "run_pipeline.ps1").read_text(encoding="utf-8")
    bash_text = (REPO_ROOT / "scripts" / "run_pipeline.sh").read_text(encoding="utf-8")

    assert "scripts\\run_pipeline.py" in powershell_text
    assert "scripts/run_pipeline.py" in bash_text


def test_ci_historical_backlog_step_uses_include_closed() -> None:
    workflow_text = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "--backlog-path planning/github-issues-backlog.md --include-closed --dry-run" in workflow_text


def test_ci_support_matrix_includes_python_312_and_macos() -> None:
    workflow_text = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "compat-linux-312" in workflow_text
    assert "compat-windows-312" in workflow_text
    assert "compat-macos-312" in workflow_text
    assert 'python-version: "3.12"' in workflow_text
    assert "runs-on: macos-latest" in workflow_text


def test_package_version_matches_pyproject_version() -> None:
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    pyproject = tomllib.loads(pyproject_text)
    init_text = (REPO_ROOT / "src" / "etl_identity_engine" / "__init__.py").read_text(
        encoding="utf-8"
    )

    project_version = pyproject["project"]["version"]
    assert f'__version__ = "{project_version}"' in init_text
