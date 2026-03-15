from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "package_customer_pilot_bundle.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
SPEC = importlib.util.spec_from_file_location("package_customer_pilot_bundle_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_build_bundle_name_uses_expected_pattern() -> None:
    assert (
        MODULE.build_bundle_name("0.9.2", "public-safety-regressions")
        == "etl-identity-engine-v0.9.2-customer-pilot-public-safety-regressions.zip"
    )


def test_build_manifest_contains_expected_fields() -> None:
    manifest = MODULE.build_manifest(
        version="0.9.2",
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
        "version": "0.9.2",
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


def test_package_customer_pilot_bundle_builds_expected_zip(tmp_path: Path) -> None:
    bundle_path = MODULE.package_customer_pilot_bundle(
        output_dir=tmp_path,
        source_manifest=Path("fixtures/public_safety_regressions/manifest.yml"),
        pilot_name="public-safety-regressions",
        version="0.9.2",
    )

    assert bundle_path.exists()

    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        expected_members = {
            "README.md",
            MODULE.MANIFEST_NAME,
            "state/pipeline_state.sqlite",
            "demo_shell/db.sqlite3",
            "demo_shell/bundle/data/public_safety_demo/public_safety_demo_summary.json",
            "launch/start_demo_shell.ps1",
            "launch/start_demo_shell.sh",
            "launch/bootstrap_windows_pilot.ps1",
            "tools/rebuild_demo_shell.py",
            "tools/bootstrap_windows_pilot.py",
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
        assert "tools/rebuild_demo_shell.py" in manifest["artifacts"]
