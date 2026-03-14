from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Sequence

from etl_identity_engine.runtime_config import load_runtime_environment
from etl_identity_engine.storage.state_store_target import resolve_state_store_target


REPO_ROOT = Path(__file__).resolve().parents[1]
BOOLEAN_TRUE_VALUES = {"1", "true", "yes", "on"}
PATH_ENV_VARS = (
    "ETL_IDENTITY_TLS_CERT_PATH",
    "ETL_IDENTITY_TLS_KEY_PATH",
    "ETL_IDENTITY_AUDIT_LOG_DIR",
    "ETL_IDENTITY_BACKUP_ROOT",
)
ATTESTATION_ENV_VARS = (
    "ETL_IDENTITY_CJIS_ENCRYPTION_AT_REST",
    "ETL_IDENTITY_CJIS_MFA_ENFORCED",
    "ETL_IDENTITY_CJIS_PERSONNEL_SCREENING",
    "ETL_IDENTITY_CJIS_SECURITY_ADDENDUM",
    "ETL_IDENTITY_CJIS_AUDIT_REVIEW",
)
STRING_ENV_VARS = ("ETL_IDENTITY_CJIS_INCIDENT_CONTACT",)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the repo-side CJIS deployment baseline for a selected runtime environment."
    )
    parser.add_argument("--environment", default="cjis")
    parser.add_argument(
        "--runtime-config",
        default=str(REPO_ROOT / "config" / "runtime_environments.yml"),
        help="Runtime environment catalog to validate.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON output path for the preflight summary.",
    )
    return parser.parse_args(argv)


def _bool_env_ok(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in BOOLEAN_TRUE_VALUES


def evaluate_cjis_preflight(
    *,
    environment_name: str,
    runtime_config_path: Path,
    environ: dict[str, str] | None = None,
) -> dict[str, object]:
    effective_environ = dict(os.environ if environ is None else environ)
    previous_env = os.environ.copy()
    os.environ.update(effective_environ)
    try:
        environment = load_runtime_environment(environment_name, runtime_config_path)
    finally:
        os.environ.clear()
        os.environ.update(previous_env)

    checks: list[dict[str, object]] = []
    errors: list[str] = []

    checks.append(
        {
            "check": "runtime_environment",
            "status": "ok" if environment.name == environment_name else "error",
            "detail": environment.name,
        }
    )

    if environment.state_db is None:
        errors.append("state_db is required for the CJIS deployment baseline")
        checks.append({"check": "state_db_present", "status": "error", "detail": "missing"})
    else:
        target = resolve_state_store_target(environment.state_db)
        is_postgresql = target.backend == "postgresql"
        checks.append(
            {
                "check": "state_db_backend",
                "status": "ok" if is_postgresql else "error",
                "detail": target.display_name,
            }
        )
        if not is_postgresql:
            errors.append("CJIS baseline requires a PostgreSQL state store, not SQLite")

    service_auth = environment.service_auth
    if service_auth is None:
        errors.append("service_auth is required for the CJIS deployment baseline")
        checks.append({"check": "service_auth", "status": "error", "detail": "missing"})
    else:
        checks.append(
            {
                "check": "service_auth_mode",
                "status": "ok" if service_auth.mode == "jwt" else "error",
                "detail": service_auth.mode,
            }
        )
        if service_auth.mode != "jwt":
            errors.append("CJIS baseline requires JWT service authentication")
        checks.append(
            {
                "check": "service_auth_header",
                "status": "ok" if service_auth.header_name == "Authorization" else "error",
                "detail": service_auth.header_name,
            }
        )
        if service_auth.header_name != "Authorization":
            errors.append("CJIS baseline requires Authorization bearer header handling")
        algorithms = tuple(service_auth.algorithms)
        checks.append(
            {
                "check": "jwt_algorithms",
                "status": "ok" if algorithms == ("RS256",) else "error",
                "detail": list(algorithms),
            }
        )
        if algorithms != ("RS256",):
            errors.append("CJIS baseline requires RS256 JWT validation in the shipped runtime profile")
        checks.append(
            {
                "check": "jwt_public_key",
                "status": "ok" if bool(service_auth.jwt_public_key_pem) else "error",
                "detail": "configured" if service_auth.jwt_public_key_pem else "missing",
            }
        )
        if not service_auth.jwt_public_key_pem:
            errors.append("CJIS baseline requires a configured JWT public key")

    for secret_key in ("object_storage_access_key", "object_storage_secret_key"):
        secret_value = environment.secrets.get(secret_key, "")
        checks.append(
            {
                "check": f"secret:{secret_key}",
                "status": "ok" if secret_value and secret_value != "disabled" else "error",
                "detail": "configured" if secret_value and secret_value != "disabled" else "missing",
            }
        )
        if not secret_value or secret_value == "disabled":
            errors.append(f"CJIS baseline requires {secret_key} to be configured")

    for env_name in PATH_ENV_VARS:
        env_value = effective_environ.get(env_name, "").strip()
        path_exists = bool(env_value) and Path(env_value).exists()
        checks.append(
            {
                "check": f"env:{env_name}",
                "status": "ok" if path_exists else "error",
                "detail": env_value or "missing",
            }
        )
        if not path_exists:
            errors.append(f"{env_name} must point to an existing path")

    for env_name in ATTESTATION_ENV_VARS:
        env_value = effective_environ.get(env_name)
        checks.append(
            {
                "check": f"env:{env_name}",
                "status": "ok" if _bool_env_ok(env_value) else "error",
                "detail": env_value or "missing",
            }
        )
        if not _bool_env_ok(env_value):
            errors.append(f"{env_name} must be set to an affirmative value")

    for env_name in STRING_ENV_VARS:
        env_value = effective_environ.get(env_name, "").strip()
        checks.append(
            {
                "check": f"env:{env_name}",
                "status": "ok" if bool(env_value) else "error",
                "detail": env_value or "missing",
            }
        )
        if not env_value:
            errors.append(f"{env_name} must be configured")

    return {
        "environment": environment_name,
        "runtime_config_path": str(runtime_config_path.resolve()),
        "status": "ok" if not errors else "error",
        "checks": checks,
        "errors": errors,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = evaluate_cjis_preflight(
        environment_name=args.environment,
        runtime_config_path=Path(args.runtime_config),
    )
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
