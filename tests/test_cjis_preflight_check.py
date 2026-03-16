from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

SCRIPT_PATH = SCRIPTS_DIR / "cjis_preflight_check.py"
SPEC = importlib.util.spec_from_file_location("cjis_preflight_check_script", SCRIPT_PATH)
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


def _write_runtime_config(path: Path, state_db: str, *, jwt_mode: bool = True) -> None:
    service_auth = """
    service_auth:
      mode: jwt
      header_name: Authorization
      issuer: ${ETL_IDENTITY_SERVICE_JWT_ISSUER}
      audience: ${ETL_IDENTITY_SERVICE_JWT_AUDIENCE}
      algorithms:
        - RS256
      jwt_public_key_pem: ${ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM}
      tenant_claim_path: tenant_id
      reader_roles:
        - etl-identity-reader
      operator_roles:
        - etl-identity-operator
""" if jwt_mode else """
    service_auth:
      mode: api_key
      header_name: X-API-Key
      reader_api_key: ${ETL_IDENTITY_SERVICE_READER_API_KEY}
      operator_api_key: ${ETL_IDENTITY_SERVICE_OPERATOR_API_KEY}
      reader_tenant_id: ${ETL_IDENTITY_SERVICE_READER_TENANT_ID:-default}
      operator_tenant_id: ${ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID:-default}
"""
    path.write_text(
        f"""
default_environment: cjis
environments:
  cjis:
    config_dir: .
    state_db: {state_db}
    secrets:
      object_storage_access_key: ${{ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY}}
      object_storage_secret_key: ${{ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY}}
{service_auth}
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_evaluate_cjis_preflight_passes_for_valid_postgresql_jwt_baseline(tmp_path: Path) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_config(runtime_config, "${ETL_IDENTITY_STATE_DB}")

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
    access_key_path = tmp_path / "object-storage-access-key.txt"
    access_key_path.write_text("access-key", encoding="utf-8")
    secret_key_path = tmp_path / "object-storage-secret-key.txt"
    secret_key_path.write_text("secret-key", encoding="utf-8")

    summary = MODULE.evaluate_cjis_preflight(
        environment_name="cjis",
        runtime_config_path=runtime_config,
        environ={
            "ETL_IDENTITY_STATE_DB": "postgresql+psycopg://etl_identity:secret@db.internal:5432/identity_state",
            "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY_FILE": str(access_key_path),
            "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY_FILE": str(secret_key_path),
            "ETL_IDENTITY_SERVICE_JWT_ISSUER": "https://issuer.example.gov",
            "ETL_IDENTITY_SERVICE_JWT_AUDIENCE": "etl-identity-api",
            "ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM_FILE": str(public_key_path),
            "ETL_IDENTITY_TLS_CERT_PATH": str(cert_path),
            "ETL_IDENTITY_TLS_KEY_PATH": str(key_path),
            "ETL_IDENTITY_AUDIT_LOG_DIR": str(audit_dir),
            "ETL_IDENTITY_BACKUP_ROOT": str(backup_dir),
            "ETL_IDENTITY_CJIS_ENCRYPTION_AT_REST": "1",
            "ETL_IDENTITY_CJIS_MFA_ENFORCED": "true",
            "ETL_IDENTITY_CJIS_PERSONNEL_SCREENING": "yes",
            "ETL_IDENTITY_CJIS_SECURITY_ADDENDUM": "1",
            "ETL_IDENTITY_CJIS_AUDIT_REVIEW": "on",
            "ETL_IDENTITY_CJIS_INCIDENT_CONTACT": "security@example.gov",
        },
    )

    assert summary["status"] == "ok"
    assert summary["errors"] == []


def test_evaluate_cjis_preflight_rejects_stale_secret_files(tmp_path: Path) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_config(runtime_config, "${ETL_IDENTITY_STATE_DB}")

    cert_path = tmp_path / "tls.crt"
    key_path = tmp_path / "tls.key"
    public_key_path = tmp_path / "jwt-public.pem"
    access_key_path = tmp_path / "object-storage-access-key.txt"
    secret_key_path = tmp_path / "object-storage-secret-key.txt"
    audit_dir = tmp_path / "audit"
    backup_dir = tmp_path / "backups"
    cert_path.write_text("certificate", encoding="utf-8")
    key_path.write_text("key", encoding="utf-8")
    public_key_path.write_text(_generate_rsa_public_key_pem(), encoding="utf-8")
    access_key_path.write_text("access-key", encoding="utf-8")
    secret_key_path.write_text("secret-key", encoding="utf-8")
    audit_dir.mkdir()
    backup_dir.mkdir()

    summary = MODULE.evaluate_cjis_preflight(
        environment_name="cjis",
        runtime_config_path=runtime_config,
        max_secret_file_age_hours=0.0,
        environ={
            "ETL_IDENTITY_STATE_DB": "postgresql+psycopg://etl_identity:secret@db.internal:5432/identity_state",
            "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY_FILE": str(access_key_path),
            "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY_FILE": str(secret_key_path),
            "ETL_IDENTITY_SERVICE_JWT_ISSUER": "https://issuer.example.gov",
            "ETL_IDENTITY_SERVICE_JWT_AUDIENCE": "etl-identity-api",
            "ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM_FILE": str(public_key_path),
            "ETL_IDENTITY_TLS_CERT_PATH": str(cert_path),
            "ETL_IDENTITY_TLS_KEY_PATH": str(key_path),
            "ETL_IDENTITY_AUDIT_LOG_DIR": str(audit_dir),
            "ETL_IDENTITY_BACKUP_ROOT": str(backup_dir),
            "ETL_IDENTITY_CJIS_ENCRYPTION_AT_REST": "1",
            "ETL_IDENTITY_CJIS_MFA_ENFORCED": "true",
            "ETL_IDENTITY_CJIS_PERSONNEL_SCREENING": "yes",
            "ETL_IDENTITY_CJIS_SECURITY_ADDENDUM": "1",
            "ETL_IDENTITY_CJIS_AUDIT_REVIEW": "on",
            "ETL_IDENTITY_CJIS_INCIDENT_CONTACT": "security@example.gov",
        },
    )

    assert summary["status"] == "error"
    assert any("secret file age" in error for error in summary["errors"])


def test_evaluate_cjis_preflight_rejects_sqlite_and_api_key_mode(tmp_path: Path) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_config(runtime_config, "./state/dev.sqlite", jwt_mode=False)

    summary = MODULE.evaluate_cjis_preflight(
        environment_name="cjis",
        runtime_config_path=runtime_config,
        environ={
            "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY": "access-key",
            "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY": "secret-key",
            "ETL_IDENTITY_SERVICE_READER_API_KEY": "reader",
            "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY": "operator",
        },
    )

    assert summary["status"] == "error"
    assert "CJIS baseline requires a PostgreSQL state store, not SQLite" in summary["errors"]
    assert "CJIS baseline requires JWT service authentication" in summary["errors"]
