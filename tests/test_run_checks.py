from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_checks.py"
SPEC = importlib.util.spec_from_file_location("run_checks_module", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_verify_installed_distribution_version_matches_pyproject(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "etl-identity-engine"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(MODULE.metadata, "version", lambda _: "1.2.3")

    assert MODULE.verify_installed_distribution_version(pyproject_path=pyproject) == (
        "1.2.3",
        "1.2.3",
    )


def test_verify_installed_distribution_version_rejects_stale_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "etl-identity-engine"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(MODULE.metadata, "version", lambda _: "1.2.2")

    with pytest.raises(SystemExit, match="Installed distribution metadata is out of date"):
        MODULE.verify_installed_distribution_version(pyproject_path=pyproject)


def test_verify_installed_distribution_version_requires_installed_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "etl-identity-engine"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )

    def raise_missing(_: str) -> str:
        raise MODULE.metadata.PackageNotFoundError

    monkeypatch.setattr(MODULE.metadata, "version", raise_missing)

    with pytest.raises(SystemExit, match="The project is not installed in the active environment"):
        MODULE.verify_installed_distribution_version(pyproject_path=pyproject)


def test_verify_distribution_build_runs_python_build_and_detects_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, ...]] = []

    def fake_run_command(command: tuple[str, ...], *, env: dict[str, str] | None = None) -> None:
        del env
        commands.append(command)
        output_dir = Path(command[-1])
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "etl_identity_engine-1.2.3.tar.gz").write_bytes(b"sdist")
        (output_dir / "etl_identity_engine-1.2.3-py3-none-any.whl").write_bytes(b"wheel")

    monkeypatch.setattr(MODULE, "_run_command", fake_run_command)

    sdist_name, wheel_name = MODULE.verify_distribution_build("python", temp_root=str(tmp_path))

    assert commands == [
        ("python", "-m", "build", "--sdist", "--wheel", "--outdir", commands[0][-1])
    ]
    assert sdist_name == "etl_identity_engine-1.2.3.tar.gz"
    assert wheel_name == "etl_identity_engine-1.2.3-py3-none-any.whl"


def test_verify_distribution_build_requires_expected_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_command(command: tuple[str, ...], *, env: dict[str, str] | None = None) -> None:
        del env
        output_dir = Path(command[-1])
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "etl_identity_engine-1.2.3-py3-none-any.whl").write_bytes(b"wheel")

    monkeypatch.setattr(MODULE, "_run_command", fake_run_command)

    with pytest.raises(
        SystemExit,
        match="Distribution build did not produce exactly one wheel and one sdist artifact",
    ):
        MODULE.verify_distribution_build("python", temp_root=str(tmp_path))
