from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import zipfile


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "check_customer_pilot_readiness.py"
)
SPEC = importlib.util.spec_from_file_location("check_customer_pilot_readiness_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _write_sample_bundle_root(root: Path) -> None:
    files = {
        "pilot_manifest.json": json.dumps(
            {
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
                "launch_helpers": ["launch/bootstrap_windows_pilot.ps1"],
                "artifacts": ["README.md", MODULE.HANDOFF_MANIFEST_NAME],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        "README.md": "# demo\n",
        "runtime/requirements-pilot.txt": "Django>=5.2,<5.3\n",
        "runtime/config/runtime_environments.yml": "default_environment: container\nenvironments: {}\n",
        "launch/bootstrap_windows_pilot.ps1": "Write-Host bootstrap\n",
        "launch/check_pilot_readiness.ps1": "Write-Host readiness\n",
        "tools/bootstrap_windows_pilot.py": "print('bootstrap')\n",
        "tools/check_pilot_readiness.py": "print('readiness')\n",
        "state/pipeline_state.sqlite": "sqlite\n",
    }
    for relative_path, contents in files.items():
        path = root.joinpath(*relative_path.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")

    artifact_entries = []
    for relative_path in sorted(files):
        path = root.joinpath(*relative_path.split("/"))
        payload = path.read_bytes()
        artifact_entries.append(
            {
                "path": relative_path,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
            }
        )
    handoff_manifest = {
        "project": "etl-identity-engine",
        "bundle_type": "customer_pilot",
        "version": "0.9.2",
        "pilot_name": "public-safety-regressions",
        "generated_at_utc": "2026-03-15T00:00:00Z",
        "source_commit": "abc123",
        "source_manifest": "seed_dataset/manifest.yml",
        "source_run_id": "RUN-EXAMPLE",
        "verification_type": "sha256",
        "artifacts": artifact_entries,
    }
    (root / MODULE.HANDOFF_MANIFEST_NAME).write_text(
        json.dumps(handoff_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_evaluate_customer_pilot_readiness_for_bundle_root(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    _write_sample_bundle_root(bundle_root)

    summary = MODULE.evaluate_customer_pilot_readiness(
        bundle_root=str(bundle_root),
        install_root=str(bundle_root),
        python_version=(3, 11, 6),
        system_name="Windows",
        docker_available=True,
        docker_server_ready=True,
        free_bytes=8 * 1024 * 1024 * 1024,
        min_free_gib=1.0,
    )

    assert summary["status"] == "ok"
    assert summary["pilot_name"] == "public-safety-regressions"
    assert summary["verification_type"] == "sha256"
    assert summary["errors"] == []


def test_evaluate_customer_pilot_readiness_for_bundle_zip(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    _write_sample_bundle_root(bundle_root)
    bundle_zip = tmp_path / "pilot.zip"
    with zipfile.ZipFile(bundle_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(bundle_root.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(bundle_root).as_posix())

    summary = MODULE.evaluate_customer_pilot_readiness(
        bundle=str(bundle_zip),
        install_root=str(tmp_path / "install"),
        python_version=(3, 13, 0),
        system_name="Windows",
        docker_available=True,
        docker_server_ready=True,
        free_bytes=8 * 1024 * 1024 * 1024,
        min_free_gib=1.0,
    )

    assert summary["status"] == "ok"
    assert summary["bundle"] == str(bundle_zip.resolve())


def test_evaluate_customer_pilot_readiness_reports_hash_mismatch(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    _write_sample_bundle_root(bundle_root)
    handoff_path = bundle_root / MODULE.HANDOFF_MANIFEST_NAME
    payload = json.loads(handoff_path.read_text(encoding="utf-8"))
    payload["artifacts"][0]["sha256"] = "bad-hash"
    handoff_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    summary = MODULE.evaluate_customer_pilot_readiness(
        bundle_root=str(bundle_root),
        install_root=str(bundle_root),
        python_version=(3, 11, 6),
        system_name="Windows",
        docker_available=True,
        docker_server_ready=True,
        free_bytes=8 * 1024 * 1024 * 1024,
        min_free_gib=1.0,
    )

    assert summary["status"] == "error"
    assert any("handoff manifest verification failed" in error for error in summary["errors"])
