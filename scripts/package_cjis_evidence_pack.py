from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Sequence
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent


def _ensure_repo_paths_on_path() -> None:
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    src_dir = REPO_ROOT / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


_ensure_repo_paths_on_path()

PROJECT_NAME = "etl-identity-engine"
DEFAULT_OUTPUT_DIR = Path("dist") / "cjis-evidence"
EVIDENCE_MANIFEST_NAME = "evidence_manifest.json"
PREFLIGHT_SUMMARY_NAME = "cjis_preflight_summary.json"
RUNTIME_SUMMARY_NAME = "runtime_environment_summary.json"
STATE_SUMMARY_NAME = "state_store_summary.json"
METRICS_SUMMARY_NAME = "operational_metrics.json"
AUDIT_EVENTS_NAME = "recent_audit_events.json"
RUN_RECORD_NAME = "selected_run_record.json"
STANDARDS_MAPPING_INDEX_NAME = "standards_mapping_index.json"
STANDARDS_MAPPING_DOC_NAME = "reference/standards-mapping.md"
CJIS_BASELINE_DOC_NAME = "reference/cjis-deployment-baseline.md"
ENV_TEMPLATE_NAME = "reference/cjis.env.example"
RUNTIME_CONFIG_SNAPSHOT_NAME = "reference/runtime_environments.yml"

BOOLEAN_TRUE_VALUES = {"1", "true", "yes", "on"}
STANDARDS_MAPPING_INDEX = (
    {
        "control_area": "Strong access control",
        "repo_baseline": "JWT-based service auth for `cjis` runtime, no API-key compatibility path, plus startup/runtime auth-material checks",
        "primary_repo_surfaces": (
            "config/runtime_environments.yml",
            "docs/runtime-environments.md",
            "scripts/cjis_preflight_check.py",
        ),
    },
    {
        "control_area": "Protected transport and host material",
        "repo_baseline": "Required TLS certificate and key paths in the preflight",
        "primary_repo_surfaces": (
            "deploy/cjis.env.example",
            "scripts/cjis_preflight_check.py",
        ),
    },
    {
        "control_area": "Audit logging",
        "repo_baseline": "Required audit-log directory plus structured logs and persisted audit events with shared free-text/auth redaction",
        "primary_repo_surfaces": (
            "docs/operations-observability.md",
            "scripts/cjis_preflight_check.py",
        ),
    },
    {
        "control_area": "Encrypted protected storage",
        "repo_baseline": "Required PostgreSQL state store and object-storage secret material, with `_FILE` mounted-secret support and optional rotation-age checks",
        "primary_repo_surfaces": (
            "config/runtime_environments.yml",
            "deploy/cjis.env.example",
            "scripts/cjis_preflight_check.py",
        ),
    },
    {
        "control_area": "Backup and recovery",
        "repo_baseline": "Required backup root plus replay/restore runbooks",
        "primary_repo_surfaces": (
            "docs/recovery-runbooks.md",
            "scripts/cjis_preflight_check.py",
        ),
    },
    {
        "control_area": "MFA and personnel attestations",
        "repo_baseline": "Required affirmative deployment attestations in the preflight",
        "primary_repo_surfaces": (
            "deploy/cjis.env.example",
            "scripts/cjis_preflight_check.py",
        ),
    },
    {
        "control_area": "Contractor / operator governance",
        "repo_baseline": "Explicit operator boundary and Security Addendum acknowledgement flag",
        "primary_repo_surfaces": (
            "SECURITY.md",
            "deploy/cjis.env.example",
            "scripts/cjis_preflight_check.py",
        ),
    },
)


def _evaluate_cjis_preflight(*args, **kwargs):
    from cjis_preflight_check import evaluate_cjis_preflight

    return evaluate_cjis_preflight(*args, **kwargs)


def _read_project_version() -> str:
    from package_release_sample import read_project_version

    return read_project_version()


def _resolve_generated_at_utc(*, explicit_value: str | None = None) -> str:
    from package_release_sample import resolve_generated_at_utc

    return resolve_generated_at_utc(repo_root=REPO_ROOT, explicit_value=explicit_value)


def _resolve_output_dir(output_dir: str) -> Path:
    from package_release_sample import resolve_output_dir

    return resolve_output_dir(output_dir)


def _resolve_source_commit() -> str:
    from package_release_sample import resolve_source_commit

    return resolve_source_commit(REPO_ROOT)


def _zip_entry_timestamp_for(generated_at_utc: str) -> tuple[int, int, int, int, int, int]:
    from package_release_sample import _zip_entry_timestamp

    return _zip_entry_timestamp(generated_at_utc)


def _load_runtime_environment(*args, **kwargs):
    from etl_identity_engine.runtime_config import load_runtime_environment

    return load_runtime_environment(*args, **kwargs)


def _current_state_store_revision(state_db: str | Path) -> str | None:
    from etl_identity_engine.storage.migration_runner import current_state_store_revision

    return current_state_store_revision(state_db)


def _head_revision() -> str:
    from etl_identity_engine.storage.migration_runner import head_revision

    return head_revision()


def _create_pipeline_state_store(state_db: str | Path):
    from etl_identity_engine.storage.sqlite_store import PipelineStateStore

    return PipelineStateStore(state_db)


def _resolve_state_store_target(state_db: str | Path):
    from etl_identity_engine.storage.state_store_target import resolve_state_store_target

    return resolve_state_store_target(state_db)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Package a CJIS evidence pack from runtime configuration, preflight output, "
            "and persisted audit artifacts."
        )
    )
    parser.add_argument("--environment", default="cjis")
    parser.add_argument(
        "--runtime-config",
        default=str(REPO_ROOT / "config" / "runtime_environments.yml"),
        help="Runtime environment catalog to inspect.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the packaged evidence zip will be written.",
    )
    parser.add_argument(
        "--state-db",
        default=None,
        help="Optional state-store override. Defaults to the selected runtime environment state_db.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional persisted run ID to include as the selected evidence run. Defaults to latest completed run.",
    )
    parser.add_argument(
        "--audit-limit",
        default=100,
        type=int,
        help="Maximum number of recent audit events to include.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional KEY=VALUE environment file to merge into the evidence-pack evaluation environment.",
    )
    parser.add_argument(
        "--max-secret-file-age-hours",
        default=None,
        type=float,
        help="Optional maximum allowed age for file-backed auth material during the included preflight.",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Version to embed in the bundle name and manifest. Defaults to pyproject.toml.",
    )
    return parser.parse_args(argv)


def build_bundle_name(version: str, environment: str) -> str:
    return f"etl-identity-engine-v{version}-cjis-evidence-{environment}.zip"


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise ValueError(f"Environment file contains a non KEY=VALUE line: {raw_line!r}")
        values[key.strip()] = value.strip()
    return values


def build_effective_environ(*, env_file: Path | None, environ: dict[str, str] | None = None) -> dict[str, str]:
    effective = dict(os.environ if environ is None else environ)
    if env_file is not None:
        effective.update(load_env_file(env_file))
    return effective


def _attestation_snapshot(environ: dict[str, str]) -> dict[str, bool]:
    keys = (
        "ETL_IDENTITY_CJIS_ENCRYPTION_AT_REST",
        "ETL_IDENTITY_CJIS_MFA_ENFORCED",
        "ETL_IDENTITY_CJIS_PERSONNEL_SCREENING",
        "ETL_IDENTITY_CJIS_SECURITY_ADDENDUM",
        "ETL_IDENTITY_CJIS_AUDIT_REVIEW",
    )
    return {
        key: str(environ.get(key, "") or "").strip().lower() in BOOLEAN_TRUE_VALUES
        for key in keys
    }


def build_runtime_environment_summary(
    *,
    environment_name: str,
    runtime_config_path: Path,
    effective_environ: dict[str, str],
) -> dict[str, object]:
    environment = _load_runtime_environment(
        environment_name,
        runtime_config_path,
        environ=effective_environ,
    )
    state_db_target = None
    if environment.state_db is not None:
        target = _resolve_state_store_target(environment.state_db)
        state_db_target = {
            "display_name": target.display_name,
            "backend": target.backend,
        }

    service_auth = environment.service_auth
    service_auth_summary: dict[str, object] | None = None
    if service_auth is not None:
        service_auth_summary = {
            "mode": service_auth.mode,
            "header_name": service_auth.header_name,
            "issuer_configured": bool(service_auth.issuer),
            "audience_configured": bool(service_auth.audience),
            "algorithms": list(service_auth.algorithms),
            "jwt_public_key_configured": bool(service_auth.jwt_public_key_pem),
            "reader_roles": list(service_auth.reader_roles),
            "operator_roles": list(service_auth.operator_roles),
            "reader_scopes": list(service_auth.reader_scopes),
            "operator_scopes": list(service_auth.operator_scopes),
            "role_claim": service_auth.role_claim,
            "scope_claim": service_auth.scope_claim,
            "subject_claim": service_auth.subject_claim,
        }
        if service_auth.tenant_claim_path is not None:
            service_auth_summary["tenant_claim_path"] = service_auth.tenant_claim_path
        if service_auth.reader_tenant_id is not None:
            service_auth_summary["reader_tenant_id"] = service_auth.reader_tenant_id
        if service_auth.operator_tenant_id is not None:
            service_auth_summary["operator_tenant_id"] = service_auth.operator_tenant_id

    return {
        "environment": environment.name,
        "runtime_config_path": str(runtime_config_path.resolve()),
        "config_dir": str(environment.config_dir),
        "state_db": state_db_target,
        "declared_secret_names": sorted(environment.secrets),
        "service_auth": service_auth_summary,
        "attestations": _attestation_snapshot(effective_environ),
        "incident_contact_configured": bool(
            str(effective_environ.get("ETL_IDENTITY_CJIS_INCIDENT_CONTACT", "") or "").strip()
        ),
    }


def _serialize_run_record(record) -> dict[str, object]:
    return {
        "run_id": record.run_id,
        "run_key": record.run_key,
        "attempt_number": record.attempt_number,
        "batch_id": record.batch_id,
        "input_mode": record.input_mode,
        "manifest_path": record.manifest_path,
        "base_dir": record.base_dir,
        "config_dir": record.config_dir,
        "profile": record.profile,
        "seed": record.seed,
        "formats": record.formats,
        "status": record.status,
        "started_at_utc": record.started_at_utc,
        "finished_at_utc": record.finished_at_utc,
        "total_records": record.total_records,
        "candidate_pair_count": record.candidate_pair_count,
        "cluster_count": record.cluster_count,
        "golden_record_count": record.golden_record_count,
        "review_queue_count": record.review_queue_count,
        "failure_detail": record.failure_detail,
        "resumed_from_run_id": record.resumed_from_run_id,
        "summary": record.summary,
    }


def _serialize_operational_metrics(metrics) -> dict[str, object]:
    return {
        "run_status_counts": metrics.run_status_counts,
        "export_status_counts": metrics.export_status_counts,
        "review_case_status_counts": metrics.review_case_status_counts,
        "audit_event_count": metrics.audit_event_count,
        "latest_completed_run_id": metrics.latest_completed_run_id,
        "latest_completed_run_finished_at_utc": metrics.latest_completed_run_finished_at_utc,
        "latest_failed_run_id": metrics.latest_failed_run_id,
        "latest_failed_run_finished_at_utc": metrics.latest_failed_run_finished_at_utc,
    }


def _serialize_audit_events(events) -> list[dict[str, object]]:
    return [
        {
            "audit_event_id": event.audit_event_id,
            "occurred_at_utc": event.occurred_at_utc,
            "actor_type": event.actor_type,
            "actor_id": event.actor_id,
            "action": event.action,
            "resource_type": event.resource_type,
            "resource_id": event.resource_id,
            "run_id": event.run_id,
            "status": event.status,
            "details": event.details,
        }
        for event in events
    ]


def _copy_reference_file(*, source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_evidence_manifest(
    *,
    version: str,
    environment_name: str,
    generated_at_utc: str,
    source_commit: str,
    runtime_config_path: Path,
    state_db_display_name: str,
    preflight_status: str,
    selected_run_id: str | None,
    artifacts: Sequence[str],
) -> dict[str, object]:
    return {
        "project": PROJECT_NAME,
        "bundle_type": "cjis_evidence_pack",
        "version": version,
        "environment": environment_name,
        "generated_at_utc": generated_at_utc,
        "source_commit": source_commit,
        "runtime_config_path": str(runtime_config_path.resolve()),
        "state_db": state_db_display_name,
        "preflight_status": preflight_status,
        "selected_run_id": selected_run_id,
        "scope_boundary": (
            "This evidence pack supports review conversations and deployment verification; "
            "it does not by itself claim full operational CJIS compliance."
        ),
        "artifacts": list(artifacts),
    }


def package_cjis_evidence_pack(
    *,
    output_dir: Path,
    environment_name: str,
    runtime_config_path: Path,
    state_db: str | Path | None,
    run_id: str | None,
    audit_limit: int,
    env_file: Path | None = None,
    environ: dict[str, str] | None = None,
    max_secret_file_age_hours: float | None = None,
    version: str,
    generated_at_utc: str | None = None,
    source_commit: str | None = None,
) -> Path:
    if audit_limit <= 0:
        raise ValueError("audit_limit must be greater than 0")

    effective_environ = build_effective_environ(env_file=env_file, environ=environ)
    runtime_summary = build_runtime_environment_summary(
        environment_name=environment_name,
        runtime_config_path=runtime_config_path,
        effective_environ=effective_environ,
    )
    runtime_environment = _load_runtime_environment(
        environment_name,
        runtime_config_path,
        environ=effective_environ,
    )

    resolved_state_db = state_db or runtime_environment.state_db
    if resolved_state_db is None:
        raise ValueError("Evidence pack requires a resolved state_db from --state-db or the runtime environment")

    preflight_summary = _evaluate_cjis_preflight(
        environment_name=environment_name,
        runtime_config_path=runtime_config_path,
        environ=effective_environ,
        max_secret_file_age_hours=max_secret_file_age_hours,
    )

    store = _create_pipeline_state_store(resolved_state_db)
    try:
        operational_metrics = store.load_operational_metrics()
        selected_run_id = run_id or store.latest_completed_run_id()
        selected_run_record = (
            None if selected_run_id is None else _serialize_run_record(store.load_run_record(selected_run_id))
        )
        audit_events = _serialize_audit_events(
            store.list_audit_events(run_id=selected_run_id, limit=audit_limit)
            if selected_run_id is not None
            else store.list_audit_events(limit=audit_limit)
        )
    finally:
        store.engine.dispose()

    target = _resolve_state_store_target(resolved_state_db)
    state_store_summary = {
        "display_name": target.display_name,
        "backend": target.backend,
        "schema_revision": _current_state_store_revision(resolved_state_db) or "uninitialized",
        "schema_head_revision": _head_revision(),
        "selected_run_id": selected_run_id,
        "audit_limit": audit_limit,
    }

    resolved_generated_at_utc = _resolve_generated_at_utc(explicit_value=generated_at_utc)
    zip_timestamp = _zip_entry_timestamp_for(resolved_generated_at_utc)
    resolved_source_commit = source_commit or _resolve_source_commit()

    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = output_dir / build_bundle_name(version, environment_name)

    with tempfile.TemporaryDirectory(prefix="etl-cjis-evidence-") as temp_dir:
        staging_root = Path(temp_dir) / "cjis_evidence"
        staging_root.mkdir(parents=True, exist_ok=True)

        _write_json(staging_root / PREFLIGHT_SUMMARY_NAME, preflight_summary)
        _write_json(staging_root / RUNTIME_SUMMARY_NAME, runtime_summary)
        _write_json(staging_root / STATE_SUMMARY_NAME, state_store_summary)
        _write_json(staging_root / METRICS_SUMMARY_NAME, _serialize_operational_metrics(operational_metrics))
        _write_json(staging_root / AUDIT_EVENTS_NAME, audit_events)
        _write_json(staging_root / STANDARDS_MAPPING_INDEX_NAME, STANDARDS_MAPPING_INDEX)
        if selected_run_record is not None:
            _write_json(staging_root / RUN_RECORD_NAME, selected_run_record)

        _copy_reference_file(
            source=REPO_ROOT / "docs" / "standards-mapping.md",
            destination=staging_root / STANDARDS_MAPPING_DOC_NAME,
        )
        _copy_reference_file(
            source=REPO_ROOT / "docs" / "cjis-deployment-baseline.md",
            destination=staging_root / CJIS_BASELINE_DOC_NAME,
        )
        _copy_reference_file(
            source=REPO_ROOT / "deploy" / "cjis.env.example",
            destination=staging_root / ENV_TEMPLATE_NAME,
        )
        _copy_reference_file(
            source=runtime_config_path,
            destination=staging_root / RUNTIME_CONFIG_SNAPSHOT_NAME,
        )

        manifest = build_evidence_manifest(
            version=version,
            environment_name=environment_name,
            generated_at_utc=resolved_generated_at_utc,
            source_commit=resolved_source_commit,
            runtime_config_path=runtime_config_path,
            state_db_display_name=target.display_name,
            preflight_status=str(preflight_summary["status"]),
            selected_run_id=selected_run_id,
            artifacts=sorted(
                str(path.relative_to(staging_root)).replace("\\", "/")
                for path in staging_root.rglob("*")
                if path.is_file()
            ),
        )
        _write_json(staging_root / EVIDENCE_MANIFEST_NAME, manifest)

        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for source_path in sorted(staging_root.rglob("*")):
                if not source_path.is_file():
                    continue
                relative_path = source_path.relative_to(staging_root).as_posix()
                zip_info = zipfile.ZipInfo(relative_path, date_time=zip_timestamp)
                zip_info.compress_type = zipfile.ZIP_DEFLATED
                zip_info.external_attr = 0o100644 << 16
                archive.writestr(zip_info, source_path.read_bytes())

    return bundle_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    bundle_path = package_cjis_evidence_pack(
        output_dir=_resolve_output_dir(args.output_dir),
        environment_name=args.environment,
        runtime_config_path=Path(args.runtime_config).resolve(),
        state_db=args.state_db,
        run_id=args.run_id,
        audit_limit=args.audit_limit,
        env_file=None if args.env_file is None else Path(args.env_file).resolve(),
        max_secret_file_age_hours=args.max_secret_file_age_hours,
        version=args.version or _read_project_version(),
    )
    print(f"CJIS evidence pack written: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
