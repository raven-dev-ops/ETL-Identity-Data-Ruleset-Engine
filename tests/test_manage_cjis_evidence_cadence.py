from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

SCRIPT_PATH = SCRIPTS_DIR / "manage_cjis_evidence_cadence.py"
SPEC = importlib.util.spec_from_file_location("manage_cjis_evidence_cadence_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _write_fake_evidence_pack(
    output_dir: Path,
    *,
    environment_name: str,
    version: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = output_dir / f"etl-identity-engine-v{version}-cjis-evidence-{environment_name}.zip"
    manifest = {
        "project": "etl-identity-engine",
        "bundle_type": "cjis_evidence_pack",
        "version": version,
        "environment": environment_name,
        "generated_at_utc": "2026-03-15T00:00:00Z",
        "source_commit": "abc123",
        "runtime_config_path": "config/runtime_environments.yml",
        "state_db": "postgresql://identity-postgres-rw:5432/identity_state",
        "preflight_status": "ok",
        "selected_run_id": "RUN-EXAMPLE",
        "scope_boundary": "review support only",
        "artifacts": [MODULE._evidence_manifest_name()],
    }
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            MODULE._evidence_manifest_name(),
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        )
    return bundle_path


def test_capture_cjis_evidence_cadence_writes_index(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        MODULE,
        "_package_cjis_evidence_pack",
        lambda **kwargs: _write_fake_evidence_pack(
            kwargs["output_dir"],
            environment_name=kwargs["environment_name"],
            version=kwargs["version"],
        ),
    )

    summary = MODULE.capture_cjis_evidence_cadence(
        output_dir=tmp_path / "review",
        environment_name="cjis",
        runtime_config_path=Path("config/runtime_environments.yml"),
        state_db=None,
        run_id=None,
        audit_limit=100,
        env_file=None,
        max_secret_file_age_hours=None,
        cadence_days=30,
        version="1.0.0",
    )

    index = summary["index"]
    assert summary["status"] == "pending"
    assert summary["capture_id"] == "20260315T000000Z"
    assert index["latest_capture_id"] == "20260315T000000Z"
    assert index["next_review_due_at_utc"] == "2026-04-14T00:00:00Z"
    assert Path(summary["index_path"]).exists()
    assert Path(summary["index_markdown_path"]).exists()


def test_review_cjis_evidence_capture_marks_latest_capture_reviewed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        MODULE,
        "_package_cjis_evidence_pack",
        lambda **kwargs: _write_fake_evidence_pack(
            kwargs["output_dir"],
            environment_name=kwargs["environment_name"],
            version=kwargs["version"],
        ),
    )
    MODULE.capture_cjis_evidence_cadence(
        output_dir=tmp_path / "review",
        environment_name="cjis",
        runtime_config_path=Path("config/runtime_environments.yml"),
        state_db=None,
        run_id=None,
        audit_limit=100,
        env_file=None,
        max_secret_file_age_hours=None,
        cadence_days=30,
        version="1.0.0",
    )

    summary = MODULE.review_cjis_evidence_capture(
        output_dir=tmp_path / "review",
        reviewer="security.analyst@example.gov",
        reviewed_at_utc="2026-03-20T12:00:00Z",
    )

    index = summary["index"]
    assert summary["status"] == "current"
    assert index["latest_reviewed_at_utc"] == "2026-03-20T12:00:00Z"
    assert index["captures"][0]["reviewer"] == "security.analyst@example.gov"
    assert index["captures"][0]["cadence_status"] == "current"
    assert index["next_review_due_at_utc"] == "2026-04-19T12:00:00Z"


def test_status_cjis_evidence_cadence_reports_overdue(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        MODULE,
        "_package_cjis_evidence_pack",
        lambda **kwargs: _write_fake_evidence_pack(
            kwargs["output_dir"],
            environment_name=kwargs["environment_name"],
            version=kwargs["version"],
        ),
    )
    MODULE.capture_cjis_evidence_cadence(
        output_dir=tmp_path / "review",
        environment_name="cjis",
        runtime_config_path=Path("config/runtime_environments.yml"),
        state_db=None,
        run_id=None,
        audit_limit=100,
        env_file=None,
        max_secret_file_age_hours=None,
        cadence_days=30,
        version="1.0.0",
    )

    status = MODULE.status_cjis_evidence_cadence(
        output_dir=tmp_path / "review",
        evaluated_at_utc="2026-05-01T00:00:00Z",
    )

    assert status["status"] == "overdue"
    assert status["overdue_capture_ids"] == ["20260315T000000Z"]
    assert status["captures"][0]["cadence_status"] == "overdue"
