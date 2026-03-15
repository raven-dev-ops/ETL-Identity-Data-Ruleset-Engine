from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "package_release_sample.py"
SPEC = importlib.util.spec_from_file_location("package_release_sample_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _write_ed25519_private_key(path: Path) -> Path:
    private_key = Ed25519PrivateKey.generate()
    path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return path


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


def test_resolve_generated_at_utc_uses_source_date_epoch() -> None:
    resolved = MODULE.resolve_generated_at_utc(
        explicit_value=None,
        environ={"SOURCE_DATE_EPOCH": "1773360000"},
    )

    assert resolved == "2026-03-13T00:00:00Z"


def test_package_release_sample_is_byte_stable_for_fixed_metadata(
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
        for relative_artifact in MODULE.RELEASE_ARTIFACTS:
            destination = base_dir / relative_artifact
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(
                f"{profile}|{seed}|{','.join(formats)}|{relative_artifact.as_posix()}\n",
                encoding="utf-8",
            )

    monkeypatch.setattr(MODULE, "_run_pipeline", fake_run_pipeline)

    bundle_one = MODULE.package_release_sample(
        output_dir=tmp_path / "first",
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version="0.1.1",
        generated_at_utc="2026-03-13T00:00:00Z",
        source_commit="abc123",
    )
    bundle_two = MODULE.package_release_sample(
        output_dir=tmp_path / "second",
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version="0.1.1",
        generated_at_utc="2026-03-13T00:00:00Z",
        source_commit="abc123",
    )

    assert bundle_one.read_bytes() == bundle_two.read_bytes()

    with zipfile.ZipFile(bundle_one) as archive:
        assert all(info.date_time == (2026, 3, 13, 0, 0, 0) for info in archive.infolist())


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


def test_package_release_sample_can_emit_detached_signature(tmp_path: Path, monkeypatch) -> None:
    signing_key_path = _write_ed25519_private_key(tmp_path / "release-signing-private.pem")

    def fake_run_pipeline(
        *,
        base_dir: Path,
        profile: str,
        seed: int,
        formats: tuple[str, ...],
        repo_root: Path,
    ) -> None:
        for relative_artifact in MODULE.RELEASE_ARTIFACTS:
            destination = base_dir / relative_artifact
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(
                f"{profile}|{seed}|{','.join(formats)}|{relative_artifact.as_posix()}\n",
                encoding="utf-8",
            )

    monkeypatch.setattr(MODULE, "_run_pipeline", fake_run_pipeline)

    bundle_path = MODULE.package_release_sample(
        output_dir=tmp_path,
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version="1.0.0",
        generated_at_utc="2026-03-15T00:00:00Z",
        source_commit="abc123",
        signing_key=signing_key_path,
        signer_identity="release-bot@example.test",
        key_id="release-ed25519",
    )

    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        assert "manifest.sig.json" in members
        signature_payload = json.loads(archive.read("manifest.sig.json").decode("utf-8"))
        assert signature_payload["manifest_path"] == MODULE.MANIFEST_NAME
        assert signature_payload["key_id"] == "release-ed25519"
        assert signature_payload["signer_identity"] == "release-bot@example.test"
