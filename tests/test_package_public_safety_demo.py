from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "package_public_safety_demo.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
SPEC = importlib.util.spec_from_file_location("package_public_safety_demo_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_build_bundle_name_uses_expected_pattern() -> None:
    assert (
        MODULE.build_bundle_name("0.6.0", "small")
        == "etl-identity-engine-v0.6.0-public-safety-demo-small.zip"
    )


def test_build_manifest_contains_expected_fields() -> None:
    manifest = MODULE.build_manifest(
        version="0.6.0",
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        generated_at_utc="2026-03-14T00:00:00Z",
        source_commit="abc123",
        artifacts=("data/example.csv",),
    )

    assert manifest == {
        "project": "etl-identity-engine",
        "bundle_type": "public_safety_demo",
        "version": "0.6.0",
        "profile": "small",
        "seed": 42,
        "formats": ["csv", "parquet"],
        "generated_at_utc": "2026-03-14T00:00:00Z",
        "source_commit": "abc123",
        "artifacts": ["data/example.csv"],
    }


def test_package_public_safety_demo_requires_csv_output(tmp_path: Path) -> None:
    try:
        MODULE.package_public_safety_demo(
            output_dir=tmp_path,
            profile="small",
            seed=42,
            formats=("parquet",),
            version="0.6.0",
        )
    except ValueError as exc:
        assert "requires csv output" in str(exc)
    else:
        raise AssertionError("Expected csv requirement to be enforced")


def test_package_public_safety_demo_is_byte_stable_for_fixed_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_run_pipeline(
        *,
        base_dir: Path,
        profile: str,
        seed: int,
        formats: tuple[str, ...],
        repo_root: Path,
    ) -> None:
        for relative_artifact in MODULE.DEMO_ARTIFACTS:
            destination = base_dir / relative_artifact
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(
                f"{profile}|{seed}|{','.join(formats)}|{relative_artifact.as_posix()}\n",
                encoding="utf-8",
            )

    monkeypatch.setattr(MODULE, "_run_pipeline", fake_run_pipeline)

    bundle_one = MODULE.package_public_safety_demo(
        output_dir=tmp_path / "first",
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version="0.6.0",
        generated_at_utc="2026-03-14T00:00:00Z",
        source_commit="abc123",
    )
    bundle_two = MODULE.package_public_safety_demo(
        output_dir=tmp_path / "second",
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version="0.6.0",
        generated_at_utc="2026-03-14T00:00:00Z",
        source_commit="abc123",
    )

    assert bundle_one.read_bytes() == bundle_two.read_bytes()

    with zipfile.ZipFile(bundle_one) as archive:
        assert all(info.date_time == (2026, 3, 14, 0, 0, 0) for info in archive.infolist())


def test_package_public_safety_demo_builds_expected_zip(tmp_path: Path) -> None:
    bundle_path = MODULE.package_public_safety_demo(
        output_dir=tmp_path,
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version="0.6.0",
    )

    assert bundle_path.exists()

    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        expected_artifacts = set(path.as_posix() for path in MODULE.DEMO_ARTIFACTS)
        assert expected_artifacts <= members
        assert MODULE.MANIFEST_NAME in members

        manifest = json.loads(archive.read(MODULE.MANIFEST_NAME).decode("utf-8"))
        assert set(manifest) == {
            "project",
            "bundle_type",
            "version",
            "profile",
            "seed",
            "formats",
            "generated_at_utc",
            "source_commit",
            "artifacts",
        }
        assert manifest["bundle_type"] == "public_safety_demo"
        assert manifest["profile"] == "small"
        assert manifest["seed"] == 42
        assert manifest["formats"] == ["csv", "parquet"]
        assert manifest["artifacts"] == [path.as_posix() for path in MODULE.DEMO_ARTIFACTS]
