from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

SCRIPT_PATH = SCRIPTS_DIR / "production_acceptance_suite.py"
SPEC = importlib.util.spec_from_file_location("production_acceptance_suite_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_evidence_pack(path: Path) -> Path:
    manifest = {
        "project": "etl-identity-engine",
        "bundle_type": "cjis_evidence_pack",
        "version": "1.0.0",
        "environment": "cjis",
        "generated_at_utc": "2026-03-15T00:00:00Z",
        "source_commit": "abc123",
        "runtime_config_path": "config/runtime_environments.yml",
        "state_db": "postgresql://identity-postgres-rw:5432/identity_state",
        "preflight_status": "ok",
        "selected_run_id": "RUN-EXAMPLE",
        "scope_boundary": "review support only",
        "artifacts": ["evidence_manifest.json"],
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("evidence_manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return path


def _write_promotion_manifest(root: Path) -> tuple[Path, Path]:
    custody_path = _write_json(root / "custody_manifest.json", {"status": "captured"})
    acceptance_path = _write_json(
        root / "acceptance_summary.json",
        {"status": "packaged", "masked_validation": {"status": "passed"}},
    )
    ha_path = _write_json(
        root / "ha_summary.json",
        {"status": "ok", "validated_steps": list(MODULE.REQUIRED_HA_STEPS)},
    )
    evidence_pack_path = _write_evidence_pack(root / "evidence.zip")
    rollback_path = root / "rollback_bundle.zip"
    rollback_path.write_bytes(b"rollback")
    promotion_manifest = {
        "status": "sealed",
        "checks": [{"check": "promotion", "status": "ok"}],
        "runtime": {
            "state_store": {
                "backend": "postgresql",
                "current_revision": "20260312_0006",
                "head_revision": "20260312_0006",
            },
            "environment_summary": {
                "service_auth": {
                    "mode": "jwt",
                    "tenant_claim_path": "tenant_id",
                }
            },
        },
        "inputs": {
            "custody_manifest": {"path": str(custody_path.resolve())},
            "acceptance_package_summary": {"path": str(acceptance_path.resolve())},
            "cjis_evidence_pack": {"path": str(evidence_pack_path.resolve())},
            "ha_rehearsal_summary": {"path": str(ha_path.resolve())},
            "rollback_bundle": {"path": str(rollback_path.resolve())},
        },
    }
    promotion_path = _write_json(root / "protected_pilot_promotion_manifest.json", promotion_manifest)
    return promotion_path, evidence_pack_path


def test_build_production_acceptance_report_is_ready_with_advisories(tmp_path: Path, monkeypatch) -> None:
    promotion_path, evidence_pack_path = _write_promotion_manifest(tmp_path)
    cadence_index_path = _write_json(
        tmp_path / "cjis_evidence_review_index.json",
        {"environment": "cjis", "cadence_days": 30, "captures": []},
    )
    monkeypatch.setattr(
        MODULE,
        "_status_cjis_evidence_cadence",
        lambda **kwargs: {
            "status": "current",
            "latest_capture_id": "20260315T000000Z",
            "captures": [{"evidence_pack_path": str(evidence_pack_path.resolve())}],
        },
    )

    summary = MODULE.build_production_acceptance_report(
        promotion_manifest_path=promotion_path,
        evidence_review_index_path=cadence_index_path,
        output_dir=tmp_path / "report",
    )

    report = summary["report"]
    assert summary["status"] == "ready_with_advisories"
    assert report["blocking_failures"] == []
    assert report["advisory_findings"] == ["service_probes"]
    assert Path(summary["report_path"]).exists()
    assert Path(summary["report_markdown_path"]).exists()


def test_build_production_acceptance_report_blocks_on_overdue_evidence(tmp_path: Path, monkeypatch) -> None:
    promotion_path, evidence_pack_path = _write_promotion_manifest(tmp_path)
    cadence_index_path = _write_json(
        tmp_path / "cjis_evidence_review_index.json",
        {"environment": "cjis", "cadence_days": 30, "captures": []},
    )
    monkeypatch.setattr(
        MODULE,
        "_status_cjis_evidence_cadence",
        lambda **kwargs: {
            "status": "overdue",
            "latest_capture_id": "20260315T000000Z",
            "captures": [{"evidence_pack_path": str(evidence_pack_path.resolve())}],
        },
    )

    summary = MODULE.build_production_acceptance_report(
        promotion_manifest_path=promotion_path,
        evidence_review_index_path=cadence_index_path,
        output_dir=tmp_path / "report",
    )

    report = summary["report"]
    assert summary["status"] == "not_ready"
    assert "evidence_cadence_current" in report["blocking_failures"]
