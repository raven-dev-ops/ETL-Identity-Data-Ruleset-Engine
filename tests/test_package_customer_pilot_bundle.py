from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import zipfile

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from etl_identity_engine.encrypted_bundle import extract_encrypted_bundle, resolve_encryption_secret


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "package_customer_pilot_bundle.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
SPEC = importlib.util.spec_from_file_location("package_customer_pilot_bundle_script", SCRIPT_PATH)
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


def test_build_bundle_name_uses_expected_pattern() -> None:
    assert (
        MODULE.build_bundle_name("1.0.0", "public-safety-regressions")
        == "etl-identity-engine-v1.0.0-customer-pilot-public-safety-regressions.zip"
    )
    assert (
        MODULE.build_bundle_name("1.0.0", "public-safety-regressions", encrypted=True)
        == "etl-identity-engine-v1.0.0-customer-pilot-public-safety-regressions-encrypted.zip"
    )


def test_build_manifest_contains_expected_fields() -> None:
    manifest = MODULE.build_manifest(
        version="1.0.0",
        pilot_name="public-safety-regressions",
        generated_at_utc="2026-03-15T00:00:00Z",
        source_commit="abc123",
        source_manifest="seed_dataset/manifest.yml",
        source_run_id="RUN-EXAMPLE",
        state_db="state/pipeline_state.sqlite",
        demo_shell_dir="demo_shell",
        launch_helpers=(
            "launch/start_demo_shell.ps1",
            "launch/start_demo_shell.sh",
            "launch/bootstrap_windows_pilot.ps1",
        ),
        artifacts=("README.md",),
    )

    assert manifest == {
        "project": "etl-identity-engine",
        "bundle_type": "customer_pilot",
        "version": "1.0.0",
        "pilot_name": "public-safety-regressions",
        "generated_at_utc": "2026-03-15T00:00:00Z",
        "source_commit": "abc123",
        "source_manifest": "seed_dataset/manifest.yml",
        "source_run_id": "RUN-EXAMPLE",
        "state_db": "state/pipeline_state.sqlite",
        "demo_shell_dir": "demo_shell",
        "launch_helpers": [
            "launch/start_demo_shell.ps1",
            "launch/start_demo_shell.sh",
            "launch/bootstrap_windows_pilot.ps1",
        ],
        "artifacts": ["README.md"],
    }


def test_build_handoff_manifest_contains_expected_fields() -> None:
    manifest = MODULE.build_handoff_manifest(
        version="1.0.0",
        pilot_name="public-safety-regressions",
        generated_at_utc="2026-03-15T00:00:00Z",
        source_commit="abc123",
        source_manifest="seed_dataset/manifest.yml",
        source_run_id="RUN-EXAMPLE",
        verification_type="sha256",
        artifacts=(
            {
                "path": "README.md",
                "sha256": "deadbeef",
                "size_bytes": 10,
            },
        ),
    )

    assert manifest == {
        "project": "etl-identity-engine",
        "bundle_type": "customer_pilot",
        "version": "1.0.0",
        "pilot_name": "public-safety-regressions",
        "generated_at_utc": "2026-03-15T00:00:00Z",
        "source_commit": "abc123",
        "source_manifest": "seed_dataset/manifest.yml",
        "source_run_id": "RUN-EXAMPLE",
        "verification_type": "sha256",
        "artifacts": [
            {
                "path": "README.md",
                "sha256": "deadbeef",
                "size_bytes": 10,
            }
        ],
    }


def test_package_customer_pilot_bundle_builds_expected_zip(tmp_path: Path) -> None:
    bundle_path = MODULE.package_customer_pilot_bundle(
        output_dir=tmp_path,
        source_manifest=Path("fixtures/public_safety_regressions/manifest.yml"),
        pilot_name="public-safety-regressions",
        version="1.0.0",
    )

    assert bundle_path.exists()

    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        expected_members = {
            "README.md",
            MODULE.MANIFEST_NAME,
            MODULE.HANDOFF_MANIFEST_NAME,
            "state/pipeline_state.sqlite",
            "demo_shell/db.sqlite3",
            "demo_shell/bundle/data/public_safety_demo/public_safety_demo_summary.json",
            "launch/start_demo_shell.ps1",
            "launch/start_demo_shell.sh",
            "launch/bootstrap_windows_pilot.ps1",
            "launch/check_pilot_readiness.ps1",
            "launch/manage_pilot_services.ps1",
            "launch/collect_support_bundle.ps1",
            "launch/patch_upgrade_pilot.ps1",
            "tools/rebuild_demo_shell.py",
            "tools/bootstrap_windows_pilot.py",
            "tools/check_pilot_readiness.py",
            "tools/manage_windows_pilot_services.py",
            "tools/package_customer_pilot_support_bundle.py",
            "tools/patch_upgrade_customer_pilot.py",
            "runtime/manage_public_safety_demo.py",
            "runtime/requirements-pilot.txt",
            "runtime/config/runtime_environments.yml",
            "runtime/src/etl_identity_engine/demo_shell/settings.py",
            "seed_dataset/manifest.yml",
            "seed_run/data/golden/golden_person_records.csv",
            "seed_run/data/public_safety_demo/public_safety_demo_summary.json",
        }
        assert expected_members <= members

        manifest = json.loads(archive.read(MODULE.MANIFEST_NAME).decode("utf-8"))
        assert set(manifest) == {
            "project",
            "bundle_type",
            "version",
            "pilot_name",
            "generated_at_utc",
            "source_commit",
            "source_manifest",
            "source_run_id",
            "state_db",
            "demo_shell_dir",
            "launch_helpers",
            "artifacts",
        }
        assert manifest["bundle_type"] == "customer_pilot"
        assert manifest["pilot_name"] == "public-safety-regressions"
        assert manifest["source_manifest"] == "seed_dataset/manifest.yml"
        assert manifest["state_db"] == "state/pipeline_state.sqlite"
        assert manifest["demo_shell_dir"] == "demo_shell"
        assert "launch/start_demo_shell.ps1" in manifest["launch_helpers"]
        assert "launch/manage_pilot_services.ps1" in manifest["launch_helpers"]
        assert "tools/rebuild_demo_shell.py" in manifest["artifacts"]
        assert "tools/package_customer_pilot_support_bundle.py" in manifest["artifacts"]

        handoff_manifest = json.loads(archive.read(MODULE.HANDOFF_MANIFEST_NAME).decode("utf-8"))
        assert handoff_manifest["verification_type"] == "sha256"
        handoff_paths = {entry["path"] for entry in handoff_manifest["artifacts"]}
        assert MODULE.MANIFEST_NAME in handoff_paths
        assert "tools/check_pilot_readiness.py" in handoff_paths
        assert "launch/patch_upgrade_pilot.ps1" in handoff_paths


def test_package_customer_pilot_bundle_can_emit_detached_signature(tmp_path: Path) -> None:
    signing_key_path = _write_ed25519_private_key(tmp_path / "pilot-signing-private.pem")

    bundle_path = MODULE.package_customer_pilot_bundle(
        output_dir=tmp_path,
        source_manifest=Path("fixtures/public_safety_regressions/manifest.yml"),
        pilot_name="public-safety-regressions",
        version="1.0.0",
        signing_key=signing_key_path,
        signer_identity="pilot-signer@example.test",
        key_id="pilot-ed25519",
    )

    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        assert MODULE.HANDOFF_SIGNATURE_NAME in members
        assert "tools/verify_handoff_signature.py" in members
        signature_payload = json.loads(archive.read(MODULE.HANDOFF_SIGNATURE_NAME).decode("utf-8"))
        assert signature_payload["manifest_path"] == MODULE.HANDOFF_MANIFEST_NAME
        assert signature_payload["key_id"] == "pilot-ed25519"
        assert signature_payload["signer_identity"] == "pilot-signer@example.test"


def test_package_customer_pilot_bundle_can_emit_encrypted_bundle(tmp_path: Path) -> None:
    passphrase_file = tmp_path / "pilot-passphrase.txt"
    passphrase_file.write_text("customer-pilot-secret\n", encoding="utf-8")
    encryption_secret = resolve_encryption_secret(passphrase_file=passphrase_file)

    bundle_path = MODULE.package_customer_pilot_bundle(
        output_dir=tmp_path,
        source_manifest=Path("fixtures/public_safety_regressions/manifest.yml"),
        pilot_name="public-safety-regressions",
        version="1.0.0",
        encryption_secret=encryption_secret,
    )

    assert bundle_path.name.endswith("-encrypted.zip")

    with tempfile.TemporaryDirectory(prefix="customer-pilot-encrypted-") as temp_dir:
        extracted_root = Path(temp_dir) / "bundle"
        summary = extract_encrypted_bundle(
            bundle_path=bundle_path,
            output_dir=extracted_root,
            encryption_secret=encryption_secret,
        )

        assert summary["bundle_type"] == "customer_pilot"
        assert (extracted_root / "README.md").exists()
        assert (extracted_root / MODULE.MANIFEST_NAME).exists()
        assert (extracted_root / MODULE.HANDOFF_MANIFEST_NAME).exists()
        assert (extracted_root / "state" / "pipeline_state.sqlite").exists()
        assert (extracted_root / "demo_shell" / "db.sqlite3").exists()
