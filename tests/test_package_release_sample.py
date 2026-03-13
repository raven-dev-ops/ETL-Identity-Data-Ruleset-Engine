from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "package_release_sample.py"
SPEC = importlib.util.spec_from_file_location("package_release_sample_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_read_project_version_reads_pyproject_version(tmp_path: Path) -> None:
    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text(
        '[project]\nname = "etl-identity-engine"\nversion = "9.9.9"\n',
        encoding="utf-8",
    )

    assert MODULE.read_project_version(pyproject_path) == "9.9.9"


def test_build_bundle_name_uses_expected_pattern() -> None:
    assert (
        MODULE.build_bundle_name("0.1.1", "small")
        == "etl-identity-engine-v0.1.1-sample-small.zip"
    )


def test_build_manifest_contains_expected_fields() -> None:
    manifest = MODULE.build_manifest(
        version="0.1.1",
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        generated_at_utc="2026-03-13T00:00:00Z",
        source_commit="abc123",
        artifacts=("data/example.csv",),
    )

    assert manifest == {
        "project": "etl-identity-engine",
        "version": "0.1.1",
        "profile": "small",
        "seed": 42,
        "formats": ["csv", "parquet"],
        "generated_at_utc": "2026-03-13T00:00:00Z",
        "source_commit": "abc123",
        "artifacts": ["data/example.csv"],
    }


def test_package_release_sample_builds_expected_zip(tmp_path: Path) -> None:
    version = MODULE.read_project_version()
    bundle_path = MODULE.package_release_sample(
        output_dir=tmp_path,
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version=version,
    )

    assert bundle_path == tmp_path / MODULE.build_bundle_name(version, "small")
    assert bundle_path.exists()

    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        expected_artifacts = set(path.as_posix() for path in MODULE.RELEASE_ARTIFACTS)
        assert expected_artifacts <= members
        assert MODULE.MANIFEST_NAME in members

        manifest = json.loads(archive.read(MODULE.MANIFEST_NAME).decode("utf-8"))
        assert set(manifest) == {
            "project",
            "version",
            "profile",
            "seed",
            "formats",
            "generated_at_utc",
            "source_commit",
            "artifacts",
        }
        assert manifest["project"] == MODULE.PROJECT_NAME
        assert manifest["version"] == version
        assert manifest["profile"] == "small"
        assert manifest["seed"] == 42
        assert manifest["formats"] == ["csv", "parquet"]
        assert manifest["artifacts"] == [path.as_posix() for path in MODULE.RELEASE_ARTIFACTS]
