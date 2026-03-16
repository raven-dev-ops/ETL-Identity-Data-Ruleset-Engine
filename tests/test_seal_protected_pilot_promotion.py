from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import zipfile

import pytest

from etl_identity_engine.ingest.landed_batch_custody import capture_live_target_custody
from etl_identity_engine.ingest.live_acceptance_package import (
    ACCEPTANCE_PACKAGE_SUMMARY_FILENAME,
    package_live_target_acceptance,
)
from etl_identity_engine.ingest.live_target_packs import prepare_live_target_pack


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

SCRIPT_PATH = SCRIPTS_DIR / "seal_protected_pilot_promotion.py"
SPEC = importlib.util.spec_from_file_location("seal_protected_pilot_promotion_script", SCRIPT_PATH)
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
                "version": "1.0.0",
                "pilot_name": "public-safety-regressions",
                "generated_at_utc": "2026-03-15T00:00:00Z",
                "source_commit": "abc123",
                "source_manifest": "seed_dataset/manifest.yml",
                "source_run_id": "RUN-EXAMPLE",
                "state_db": "state/pipeline_state.sqlite",
                "demo_shell_dir": "demo_shell",
                "launch_helpers": ["launch/bootstrap_windows_pilot.ps1"],
                "artifacts": ["README.md", "pilot_handoff_manifest.json"],
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
        "launch/manage_pilot_services.ps1": "Write-Host manage\n",
        "launch/collect_support_bundle.ps1": "Write-Host support\n",
        "launch/patch_upgrade_pilot.ps1": "Write-Host patch-upgrade\n",
        "tools/bootstrap_windows_pilot.py": "print('bootstrap')\n",
        "tools/check_pilot_readiness.py": "print('readiness')\n",
        "tools/manage_windows_pilot_services.py": "print('manage')\n",
        "tools/package_customer_pilot_support_bundle.py": "print('support')\n",
        "tools/patch_upgrade_customer_pilot.py": "print('patch-upgrade')\n",
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
        "version": "1.0.0",
        "pilot_name": "public-safety-regressions",
        "generated_at_utc": "2026-03-15T00:00:00Z",
        "source_commit": "abc123",
        "source_manifest": "seed_dataset/manifest.yml",
        "source_run_id": "RUN-EXAMPLE",
        "verification_type": "sha256",
        "artifacts": artifact_entries,
    }
    (root / "pilot_handoff_manifest.json").write_text(
        json.dumps(handoff_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_bundle_zip(root: Path, destination: Path) -> Path:
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(root).as_posix())
    return destination


def _write_evidence_pack(path: Path, *, environment: str) -> Path:
    manifest = {
        "project": "etl-identity-engine",
        "bundle_type": "cjis_evidence_pack",
        "version": "1.0.0",
        "environment": environment,
        "generated_at_utc": "2026-03-15T00:00:00Z",
        "source_commit": "abc123",
        "runtime_config_path": "config/runtime_environments.yml",
        "state_db": "postgresql://identity-postgres-rw/identity_state",
        "preflight_status": "ok",
        "selected_run_id": "RUN-EXAMPLE",
        "scope_boundary": "review support only",
        "artifacts": ["evidence_manifest.json"],
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("evidence_manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return path


def _write_runtime_env(path: Path) -> Path:
    path.write_text(
        "\n".join(
            [
                "ETL_IDENTITY_STATE_DB=postgresql://etl_identity:secret@identity-postgres-rw:5432/identity_state?target_session_attrs=read-write",
                "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY=object-access",
                "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY=object-secret",
                "ETL_IDENTITY_SERVICE_READER_API_KEY=reader-secret",
                "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY=operator-secret",
                "ETL_IDENTITY_SERVICE_READER_TENANT_ID=tenant-a",
                "ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID=tenant-a",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _write_ha_summary(path: Path, *, missing_last_step: bool = False) -> Path:
    steps = list(MODULE.REQUIRED_HA_REHEARSAL_STEPS)
    if missing_last_step:
        steps.pop()
    path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_id": "RUN-EXAMPLE",
                "replay_run_id": "RUN-REPLAY-001",
                "validated_steps": steps,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _prepare_live_inputs(tmp_path: Path) -> tuple[Path, Path]:
    staged_root = tmp_path / "prepared_cad"
    captured_root = tmp_path / "captured"
    acceptance_root = tmp_path / "acceptance"
    prepare_live_target_pack("cad_county_dispatch_v1", staged_root)
    custody_summary = capture_live_target_custody(
        "cad_county_dispatch_v1",
        staged_root,
        captured_root,
        operator_id="dispatch.operator",
        transport_channel="sftp",
        tenant_id="tenant-a",
    )
    acceptance_summary = package_live_target_acceptance(
        "cad_county_dispatch_v1",
        Path(custody_summary["immutable_root"]),
        acceptance_root,
    )
    return (
        Path(custody_summary["custody_manifest_path"]),
        Path(acceptance_summary["acceptance_root"]) / ACCEPTANCE_PACKAGE_SUMMARY_FILENAME,
    )


def _patch_state_store(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Target:
        display_name = "postgresql://etl_identity@identity-postgres-rw:5432/identity_state"
        backend = "postgresql"

    monkeypatch.setattr(MODULE, "_resolve_source_commit", lambda: "abc123")
    monkeypatch.setattr(MODULE, "_resolve_state_store_target", lambda state_db: _Target())
    monkeypatch.setattr(MODULE, "_current_state_store_revision", lambda state_db: "20260312_0006")
    monkeypatch.setattr(MODULE, "_head_revision", lambda: "20260312_0006")


def test_seal_protected_pilot_promotion_writes_manifest_and_supporting_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_state_store(monkeypatch)
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    _write_sample_bundle_root(bundle_root)
    bundle_zip = _write_bundle_zip(bundle_root, tmp_path / "pilot.zip")
    custody_manifest_path, acceptance_summary_path = _prepare_live_inputs(tmp_path)
    evidence_pack_path = _write_evidence_pack(tmp_path / "cjis-evidence.zip", environment="cluster_ha")
    ha_summary_path = _write_ha_summary(tmp_path / "ha-summary.json")
    runtime_env_path = _write_runtime_env(tmp_path / "protected-pilot.env")
    rollback_bundle_path = tmp_path / "rollback-bundle.zip"
    rollback_bundle_path.write_bytes(b"rollback bundle")

    summary = MODULE.seal_protected_pilot_promotion(
        output_dir=tmp_path / "sealed-promotions",
        bundle=bundle_zip,
        bundle_root=None,
        trusted_public_key=None,
        runtime_config_path=Path("config/runtime_environments.yml"),
        environment_name="cluster_ha",
        env_file=runtime_env_path,
        state_db=None,
        custody_manifest_path=custody_manifest_path,
        acceptance_summary_path=acceptance_summary_path,
        evidence_pack_path=evidence_pack_path,
        ha_rehearsal_summary_path=ha_summary_path,
        rollback_bundle_path=rollback_bundle_path,
    )

    assert summary["status"] == "sealed"
    promotion_root = Path(summary["promotion_root"])
    manifest = json.loads(Path(summary["promotion_manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["bundle_type"] == "protected_pilot_promotion"
    assert manifest["environment"] == "cluster_ha"
    assert manifest["target_id"] == "cad_county_dispatch_v1"
    assert manifest["tenant_id"] == "tenant-a"
    assert manifest["runtime"]["state_store"]["backend"] == "postgresql"
    assert manifest["inputs"]["custody_manifest"]["target_id"] == "cad_county_dispatch_v1"
    assert manifest["inputs"]["cjis_evidence_pack"]["preflight_status"] == "ok"
    assert (promotion_root / MODULE.RUNTIME_ENV_FINGERPRINT_NAME).exists()
    assert (promotion_root / MODULE.RUNTIME_CONFIG_SNAPSHOT_NAME).exists()
    assert (promotion_root / MODULE.CUSTODY_MANIFEST_COPY_NAME).exists()
    assert (promotion_root / MODULE.ACCEPTANCE_SUMMARY_COPY_NAME).exists()
    assert (promotion_root / MODULE.HA_REHEARSAL_SUMMARY_COPY_NAME).exists()
    assert (promotion_root / MODULE.EVIDENCE_MANIFEST_COPY_NAME).exists()
    assert (promotion_root / MODULE.PROMOTION_SUMMARY_NAME).exists()


def test_seal_protected_pilot_promotion_rejects_incomplete_ha_rehearsal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_state_store(monkeypatch)
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    _write_sample_bundle_root(bundle_root)
    bundle_zip = _write_bundle_zip(bundle_root, tmp_path / "pilot.zip")
    custody_manifest_path, acceptance_summary_path = _prepare_live_inputs(tmp_path)
    evidence_pack_path = _write_evidence_pack(tmp_path / "cjis-evidence.zip", environment="cluster_ha")
    ha_summary_path = _write_ha_summary(tmp_path / "ha-summary.json", missing_last_step=True)
    runtime_env_path = _write_runtime_env(tmp_path / "protected-pilot.env")
    rollback_bundle_path = tmp_path / "rollback-bundle.zip"
    rollback_bundle_path.write_bytes(b"rollback bundle")

    with pytest.raises(MODULE.ProtectedPilotPromotionError, match="missing required validated steps"):
        MODULE.seal_protected_pilot_promotion(
            output_dir=tmp_path / "sealed-promotions",
            bundle=bundle_zip,
            bundle_root=None,
            trusted_public_key=None,
            runtime_config_path=Path("config/runtime_environments.yml"),
            environment_name="cluster_ha",
            env_file=runtime_env_path,
            state_db=None,
            custody_manifest_path=custody_manifest_path,
            acceptance_summary_path=acceptance_summary_path,
            evidence_pack_path=evidence_pack_path,
            ha_rehearsal_summary_path=ha_summary_path,
            rollback_bundle_path=rollback_bundle_path,
        )
