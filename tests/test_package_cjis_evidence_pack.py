from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from etl_identity_engine.storage.sqlite_store import PersistRunMetadata, PipelineStateStore


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "package_cjis_evidence_pack.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
SPEC = importlib.util.spec_from_file_location("package_cjis_evidence_pack_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _generate_rsa_public_key_pem() -> str:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")


def _write_runtime_config(path: Path) -> None:
    path.write_text(
        """
default_environment: cjis
environments:
  cjis:
    config_dir: .
    state_db: ${ETL_IDENTITY_STATE_DB}
    secrets:
      object_storage_access_key: ${ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY}
      object_storage_secret_key: ${ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY}
    service_auth:
      mode: jwt
      header_name: Authorization
      issuer: ${ETL_IDENTITY_SERVICE_JWT_ISSUER}
      audience: ${ETL_IDENTITY_SERVICE_JWT_AUDIENCE}
      algorithms:
        - RS256
      jwt_public_key_pem: ${ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM}
      reader_roles:
        - etl-identity-reader
      operator_roles:
        - etl-identity-operator
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_env_file(
    path: Path,
    *,
    state_db: Path,
    public_key_path: Path,
    cert_path: Path,
    key_path: Path,
    audit_dir: Path,
    backup_dir: Path,
) -> None:
    path.write_text(
        "\n".join(
            (
                f"ETL_IDENTITY_STATE_DB={state_db}",
                "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY=access-key",
                "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY=secret-key",
                "ETL_IDENTITY_SERVICE_JWT_ISSUER=https://issuer.example.gov",
                "ETL_IDENTITY_SERVICE_JWT_AUDIENCE=etl-identity-api",
                f"ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM_FILE={public_key_path}",
                f"ETL_IDENTITY_TLS_CERT_PATH={cert_path}",
                f"ETL_IDENTITY_TLS_KEY_PATH={key_path}",
                f"ETL_IDENTITY_AUDIT_LOG_DIR={audit_dir}",
                f"ETL_IDENTITY_BACKUP_ROOT={backup_dir}",
                "ETL_IDENTITY_CJIS_ENCRYPTION_AT_REST=1",
                "ETL_IDENTITY_CJIS_MFA_ENFORCED=true",
                "ETL_IDENTITY_CJIS_PERSONNEL_SCREENING=yes",
                "ETL_IDENTITY_CJIS_SECURITY_ADDENDUM=1",
                "ETL_IDENTITY_CJIS_AUDIT_REVIEW=on",
                "ETL_IDENTITY_CJIS_INCIDENT_CONTACT=security@example.gov",
            )
        )
        + "\n",
        encoding="utf-8",
    )


def _seed_state_store(db_path: Path) -> str:
    store = PipelineStateStore(db_path)
    try:
        started_at_utc = "2026-03-15T04:00:00Z"
        finished_at_utc = "2026-03-15T04:05:00Z"
        start = store.begin_run(
            run_key="manifest::fixtures/public_safety_regressions/manifest.yml",
            batch_id="BATCH-001",
            input_mode="manifest",
            manifest_path="fixtures/public_safety_regressions/manifest.yml",
            base_dir=str(db_path.parent / "run"),
            config_dir=str(Path("config").resolve()),
            profile="small",
            seed=42,
            formats="csv",
            started_at_utc=started_at_utc,
        )
        summary = {
            "total_records": 4,
            "candidate_pair_count": 1,
            "cluster_count": 3,
            "golden_record_count": 3,
            "review_queue_count": 0,
            "public_safety_demo": {
                "incident_count": 2,
                "linked_golden_person_count": 1,
            },
        }
        metadata = PersistRunMetadata(
            run_id=start.run_id,
            run_key=start.run_key,
            attempt_number=start.attempt_number,
            batch_id="BATCH-001",
            input_mode="manifest",
            manifest_path="fixtures/public_safety_regressions/manifest.yml",
            base_dir=str(db_path.parent / "run"),
            config_dir=str(Path("config").resolve()),
            profile="small",
            seed=42,
            formats="csv",
            started_at_utc=started_at_utc,
            finished_at_utc=finished_at_utc,
            status="completed",
        )
        store.persist_run(
            metadata=metadata,
            normalized_rows=[],
            match_rows=[],
            blocking_metrics_rows=[],
            cluster_rows=[],
            golden_rows=[],
            crosswalk_rows=[],
            review_rows=[],
            public_safety_incident_identity_rows=[],
            public_safety_golden_activity_rows=[],
            summary=summary,
        )
        store.record_audit_event(
            actor_type="service",
            actor_id="etl-identity-api",
            action="publish_run",
            resource_type="run",
            resource_id=start.run_id,
            run_id=start.run_id,
            status="succeeded",
            details={"operator_notes": "Sensitive notes should not be preserved verbatim."},
        )
        return start.run_id
    finally:
        store.engine.dispose()


def test_build_runtime_environment_summary_redacts_auth_material(tmp_path: Path) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_config(runtime_config)

    public_key_path = tmp_path / "jwt-public.pem"
    public_key_path.write_text(_generate_rsa_public_key_pem(), encoding="utf-8")

    summary = MODULE.build_runtime_environment_summary(
        environment_name="cjis",
        runtime_config_path=runtime_config,
        effective_environ={
            "ETL_IDENTITY_STATE_DB": str(tmp_path / "state.sqlite"),
            "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY": "access-key",
            "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY": "secret-key",
            "ETL_IDENTITY_SERVICE_JWT_ISSUER": "https://issuer.example.gov",
            "ETL_IDENTITY_SERVICE_JWT_AUDIENCE": "etl-identity-api",
            "ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM_FILE": str(public_key_path),
            "ETL_IDENTITY_CJIS_ENCRYPTION_AT_REST": "1",
            "ETL_IDENTITY_CJIS_MFA_ENFORCED": "1",
            "ETL_IDENTITY_CJIS_PERSONNEL_SCREENING": "1",
            "ETL_IDENTITY_CJIS_SECURITY_ADDENDUM": "1",
            "ETL_IDENTITY_CJIS_AUDIT_REVIEW": "1",
            "ETL_IDENTITY_CJIS_INCIDENT_CONTACT": "security@example.gov",
        },
    )

    assert summary["declared_secret_names"] == [
        "object_storage_access_key",
        "object_storage_secret_key",
    ]
    assert summary["service_auth"] == {
        "mode": "jwt",
        "header_name": "Authorization",
        "issuer_configured": True,
        "audience_configured": True,
        "algorithms": ["RS256"],
        "jwt_public_key_configured": True,
        "reader_roles": ["etl-identity-reader"],
        "operator_roles": ["etl-identity-operator"],
        "reader_scopes": [
            "service:health",
            "service:metrics",
            "runs:read",
            "golden:read",
            "crosswalk:read",
            "public_safety:read",
            "review_cases:read",
        ],
        "operator_scopes": [
            "service:health",
            "service:metrics",
            "runs:read",
            "golden:read",
            "crosswalk:read",
            "public_safety:read",
            "review_cases:read",
            "audit_events:read",
            "runs:replay",
            "runs:publish",
            "review_cases:write",
            "exports:run",
        ],
        "role_claim": "roles",
        "scope_claim": "scope",
        "subject_claim": "sub",
    }
    assert "BEGIN PUBLIC KEY" not in json.dumps(summary, sort_keys=True)
    assert "access-key" not in json.dumps(summary, sort_keys=True)


def test_package_cjis_evidence_pack_builds_expected_zip(tmp_path: Path) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_config(runtime_config)
    state_db = tmp_path / "state" / "pipeline_state.sqlite"
    run_id = _seed_state_store(state_db)

    cert_path = tmp_path / "tls.crt"
    key_path = tmp_path / "tls.key"
    public_key_path = tmp_path / "jwt-public.pem"
    audit_dir = tmp_path / "audit"
    backup_dir = tmp_path / "backups"
    cert_path.write_text("certificate", encoding="utf-8")
    key_path.write_text("key", encoding="utf-8")
    public_key_path.write_text(_generate_rsa_public_key_pem(), encoding="utf-8")
    audit_dir.mkdir()
    backup_dir.mkdir()

    env_file = tmp_path / "cjis.env"
    _write_env_file(
        env_file,
        state_db=state_db,
        public_key_path=public_key_path,
        cert_path=cert_path,
        key_path=key_path,
        audit_dir=audit_dir,
        backup_dir=backup_dir,
    )

    bundle_path = MODULE.package_cjis_evidence_pack(
        output_dir=tmp_path,
        environment_name="cjis",
        runtime_config_path=runtime_config,
        state_db=None,
        run_id=run_id,
        audit_limit=10,
        env_file=env_file,
        version="1.0.0",
    )

    assert bundle_path.exists()
    assert bundle_path.name == "etl-identity-engine-v1.0.0-cjis-evidence-cjis.zip"

    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        expected_members = {
            MODULE.EVIDENCE_MANIFEST_NAME,
            MODULE.PREFLIGHT_SUMMARY_NAME,
            MODULE.RUNTIME_SUMMARY_NAME,
            MODULE.STATE_SUMMARY_NAME,
            MODULE.METRICS_SUMMARY_NAME,
            MODULE.AUDIT_EVENTS_NAME,
            MODULE.RUN_RECORD_NAME,
            MODULE.STANDARDS_MAPPING_INDEX_NAME,
            MODULE.STANDARDS_MAPPING_DOC_NAME,
            MODULE.CJIS_BASELINE_DOC_NAME,
            MODULE.ENV_TEMPLATE_NAME,
            MODULE.RUNTIME_CONFIG_SNAPSHOT_NAME,
        }
        assert expected_members <= members

        manifest = json.loads(archive.read(MODULE.EVIDENCE_MANIFEST_NAME).decode("utf-8"))
        assert manifest["bundle_type"] == "cjis_evidence_pack"
        assert manifest["environment"] == "cjis"
        assert manifest["preflight_status"] == "error"
        assert manifest["selected_run_id"] == run_id
        assert "does not by itself claim full operational CJIS compliance" in manifest["scope_boundary"]
        assert MODULE.STANDARDS_MAPPING_INDEX_NAME in manifest["artifacts"]

        preflight_summary = json.loads(archive.read(MODULE.PREFLIGHT_SUMMARY_NAME).decode("utf-8"))
        assert preflight_summary["status"] == "error"
        assert "CJIS baseline requires a PostgreSQL state store, not SQLite" in preflight_summary["errors"]

        runtime_summary = json.loads(archive.read(MODULE.RUNTIME_SUMMARY_NAME).decode("utf-8"))
        assert runtime_summary["service_auth"]["jwt_public_key_configured"] is True
        assert runtime_summary["state_db"]["backend"] == "sqlite"
        assert "BEGIN PUBLIC KEY" not in json.dumps(runtime_summary, sort_keys=True)

        state_summary = json.loads(archive.read(MODULE.STATE_SUMMARY_NAME).decode("utf-8"))
        assert state_summary["backend"] == "sqlite"
        assert state_summary["selected_run_id"] == run_id
        assert state_summary["schema_revision"] == state_summary["schema_head_revision"]

        operational_metrics = json.loads(archive.read(MODULE.METRICS_SUMMARY_NAME).decode("utf-8"))
        assert operational_metrics["audit_event_count"] == 1
        assert operational_metrics["latest_completed_run_id"] == run_id

        audit_events = json.loads(archive.read(MODULE.AUDIT_EVENTS_NAME).decode("utf-8"))
        assert len(audit_events) == 1
        assert audit_events[0]["run_id"] == run_id
        assert audit_events[0]["details"]["operator_notes"].startswith("[REDACTED free_text")

        run_record = json.loads(archive.read(MODULE.RUN_RECORD_NAME).decode("utf-8"))
        assert run_record["run_id"] == run_id
        assert run_record["summary"]["public_safety_demo"]["linked_golden_person_count"] == 1

        standards_mapping_index = json.loads(
            archive.read(MODULE.STANDARDS_MAPPING_INDEX_NAME).decode("utf-8")
        )
        assert standards_mapping_index[0]["control_area"] == "Strong access control"
        assert "scripts/cjis_preflight_check.py" in standards_mapping_index[0]["primary_repo_surfaces"]
