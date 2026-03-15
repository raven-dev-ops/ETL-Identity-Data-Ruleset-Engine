from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Sequence
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = Path("dist") / "customer-pilot-support"
SUPPORT_MANIFEST_NAME = "support_bundle_manifest.json"


def _ensure_runtime_src_on_path(bundle_root: Path | None = None) -> None:
    candidate_paths = [REPO_ROOT / "src", REPO_ROOT / "runtime" / "src"]
    if bundle_root is not None:
        candidate_paths.insert(0, bundle_root / "runtime" / "src")
    for candidate in candidate_paths:
        if candidate.exists() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
            return


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect a redacted troubleshooting bundle for the supported customer pilot baseline."
    )
    parser.add_argument("--bundle-root", default=None, help="Extracted customer pilot bundle root.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the support bundle zip will be written.",
    )
    parser.add_argument(
        "--state-db",
        default=None,
        help="Optional persisted state store override. Defaults to runtime/pilot_bootstrap.json when present, otherwise state/pipeline_state.sqlite.",
    )
    parser.add_argument("--audit-event-limit", default=50, type=int)
    parser.add_argument("--run-limit", default=10, type=int)
    return parser.parse_args(argv)


def _resolve_bundle_root(bundle_root: str | None) -> Path:
    resolved = Path(bundle_root).resolve() if bundle_root else Path.cwd().resolve()
    if not (resolved / "pilot_manifest.json").exists():
        raise FileNotFoundError(
            "Unable to locate an extracted customer pilot bundle root. Provide --bundle-root."
        )
    return resolved


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _resolve_state_db(bundle_root: Path, explicit_state_db: str | None) -> str:
    if explicit_state_db:
        return explicit_state_db
    bootstrap_config_path = bundle_root / "runtime" / "pilot_bootstrap.json"
    if bootstrap_config_path.exists():
        bootstrap_config = _read_json(bootstrap_config_path)
        state_db = str(bootstrap_config.get("state_db", "")).strip()
        if state_db:
            return state_db
    return str((bundle_root / "state" / "pipeline_state.sqlite").resolve())


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sanitize_mapping(value: dict[str, object], *, bundle_root: Path) -> dict[str, Any]:
    _ensure_runtime_src_on_path(bundle_root)
    from etl_identity_engine.observability import sanitize_observability_fields

    return sanitize_observability_fields(value)


def _sanitize_runtime_env_mapping(value: dict[str, str], *, bundle_root: Path) -> dict[str, Any]:
    sanitized = _sanitize_mapping(value, bundle_root=bundle_root)
    redacted_suffixes = (
        "_API_KEY",
        "_PASSWORD",
        "_SECRET",
        "_JWT",
        "_JWT_SECRET",
        "_JWT_PUBLIC_KEY_PEM",
        "_TOKEN",
    )
    for key in value:
        normalized_key = key.strip().upper()
        if normalized_key.endswith(redacted_suffixes):
            sanitized[key] = "[REDACTED auth_material]"
    return sanitized


def _sanitize_text(value: str, *, bundle_root: Path) -> str:
    sanitized = _sanitize_mapping({"log_text": value}, bundle_root=bundle_root)
    return str(sanitized["log_text"])


def _copy_redacted_logs(*, bundle_root: Path, staging_root: Path) -> list[str]:
    log_root = bundle_root / "runtime" / "logs"
    relative_paths: list[str] = []
    if not log_root.exists():
        return relative_paths
    for source_path in sorted(log_root.rglob("*.log")):
        if not source_path.is_file():
            continue
        relative_path = Path("logs") / source_path.relative_to(log_root)
        destination = staging_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            _sanitize_text(source_path.read_text(encoding="utf-8", errors="replace"), bundle_root=bundle_root),
            encoding="utf-8",
        )
        relative_paths.append(relative_path.as_posix())
    return relative_paths


def _collect_state_metadata(
    *,
    bundle_root: Path,
    state_db: str,
    audit_event_limit: int,
    run_limit: int,
    staging_root: Path,
) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    written_paths: list[str] = []
    try:
        _ensure_runtime_src_on_path(bundle_root)
        from etl_identity_engine.runtime_config import evaluate_runtime_auth_material
        from etl_identity_engine.storage.migration_runner import current_state_store_revision, head_revision
        from etl_identity_engine.storage.sqlite_store import PipelineStateStore
        from etl_identity_engine.storage.state_store_target import state_store_display_name

        runtime_env = _read_env_file(bundle_root / "runtime" / "pilot_runtime.env")
        runtime_config_path = bundle_root / "runtime" / "config" / "runtime_environments.yml"
        auth_summary = evaluate_runtime_auth_material(
            "container",
            runtime_config_path if runtime_config_path.exists() else None,
            environ={**os.environ, **runtime_env},
            include_declared_secrets=True,
        )
        auth_summary_path = staging_root / "runtime" / "runtime_auth_material.json"
        _write_json(auth_summary_path, auth_summary)
        written_paths.append(auth_summary_path.relative_to(staging_root).as_posix())

        state_metadata = {
            "state_db": _sanitize_mapping({"state_db": state_db}, bundle_root=bundle_root)["state_db"],
            "display_name": state_store_display_name(state_db),
            "schema_current_revision": current_state_store_revision(state_db),
            "schema_head_revision": head_revision(),
        }
        store = PipelineStateStore(state_db)
        try:
            metrics = store.load_operational_metrics()
            recent_runs = [
                asdict(record)
                for record in store.list_run_records(limit=run_limit, offset=0).items
            ]
            recent_audit_events = [
                asdict(event)
                for event in store.list_audit_events(limit=audit_event_limit)
            ]
        finally:
            store.engine.dispose()
        state_metadata_path = staging_root / "state" / "state_metadata.json"
        _write_json(state_metadata_path, state_metadata)
        written_paths.append(state_metadata_path.relative_to(staging_root).as_posix())
        metrics_path = staging_root / "state" / "operational_metrics.json"
        _write_json(metrics_path, asdict(metrics))
        written_paths.append(metrics_path.relative_to(staging_root).as_posix())
        runs_path = staging_root / "state" / "recent_runs.json"
        _write_json(runs_path, recent_runs)
        written_paths.append(runs_path.relative_to(staging_root).as_posix())
        audit_path = staging_root / "state" / "recent_audit_events.json"
        _write_json(audit_path, recent_audit_events)
        written_paths.append(audit_path.relative_to(staging_root).as_posix())
    except Exception as exc:  # pragma: no cover - exercised by failure scenarios.
        warnings.append(f"state metadata collection failed: {exc}")
    return written_paths, warnings


def _collect_service_status(*, bundle_root: Path, staging_root: Path) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    try:
        _ensure_runtime_src_on_path(bundle_root)
        from etl_identity_engine.windows_pilot_services import query_windows_pilot_service_status

        statuses = {
            kind: asdict(query_windows_pilot_service_status(kind, bundle_root=bundle_root))
            for kind in ("demo_shell", "service_api")
        }
    except Exception as exc:
        warnings.append(f"windows service status collection skipped: {exc}")
        return [], warnings

    destination = staging_root / "runtime" / "windows_service_status.json"
    _write_json(destination, statuses)
    return [destination.relative_to(staging_root).as_posix()], warnings


def _zip_entry_timestamp() -> tuple[int, int, int, int, int, int]:
    now = datetime.now(timezone.utc)
    return (now.year, now.month, now.day, now.hour, now.minute, now.second)


def build_support_bundle_name(*, version: str, pilot_name: str) -> str:
    return f"etl-identity-engine-v{version}-customer-pilot-support-{pilot_name}.zip"


def package_customer_pilot_support_bundle(
    *,
    bundle_root: Path,
    output_dir: Path,
    state_db: str | None,
    audit_event_limit: int,
    run_limit: int,
) -> Path:
    pilot_manifest = _read_json(bundle_root / "pilot_manifest.json")
    pilot_name = str(pilot_manifest.get("pilot_name", "customer-pilot")).strip() or "customer-pilot"
    version = str(pilot_manifest.get("version", "unknown")).strip() or "unknown"
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = output_dir / build_support_bundle_name(version=version, pilot_name=pilot_name)
    resolved_state_db = _resolve_state_db(bundle_root, state_db)

    with tempfile.TemporaryDirectory(prefix="etl-customer-pilot-support-") as temp_dir:
        staging_root = Path(temp_dir) / "support_bundle"
        staging_root.mkdir(parents=True, exist_ok=True)

        written_paths: list[str] = []
        warnings: list[str] = []
        generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        for relative_path in (
            "pilot_manifest.json",
            "pilot_handoff_manifest.json",
            "pilot_handoff_manifest.sig.json",
        ):
            source_path = bundle_root / relative_path
            if source_path.exists():
                destination = staging_root / relative_path
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, destination)
                written_paths.append(relative_path)

        bootstrap_path = bundle_root / "runtime" / "pilot_bootstrap.json"
        if bootstrap_path.exists():
            redacted_bootstrap = _sanitize_mapping(_read_json(bootstrap_path), bundle_root=bundle_root)
            destination = staging_root / "runtime" / "pilot_bootstrap.redacted.json"
            _write_json(destination, redacted_bootstrap)
            written_paths.append(destination.relative_to(staging_root).as_posix())

        runtime_env_path = bundle_root / "runtime" / "pilot_runtime.env"
        if runtime_env_path.exists():
            redacted_env = _sanitize_runtime_env_mapping(
                _read_env_file(runtime_env_path),
                bundle_root=bundle_root,
            )
            destination = staging_root / "runtime" / "pilot_runtime.redacted.json"
            _write_json(destination, redacted_env)
            written_paths.append(destination.relative_to(staging_root).as_posix())

        runtime_config_path = bundle_root / "runtime" / "config" / "runtime_environments.yml"
        if runtime_config_path.exists():
            destination = staging_root / "runtime" / "runtime_environments.yml"
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(
                _sanitize_text(runtime_config_path.read_text(encoding="utf-8"), bundle_root=bundle_root),
                encoding="utf-8",
            )
            written_paths.append(destination.relative_to(staging_root).as_posix())

        written_paths.extend(_copy_redacted_logs(bundle_root=bundle_root, staging_root=staging_root))
        state_paths, state_warnings = _collect_state_metadata(
            bundle_root=bundle_root,
            state_db=resolved_state_db,
            audit_event_limit=audit_event_limit,
            run_limit=run_limit,
            staging_root=staging_root,
        )
        written_paths.extend(state_paths)
        warnings.extend(state_warnings)
        service_paths, service_warnings = _collect_service_status(
            bundle_root=bundle_root,
            staging_root=staging_root,
        )
        written_paths.extend(service_paths)
        warnings.extend(service_warnings)

        support_manifest = {
            "project": "etl-identity-engine",
            "bundle_type": "customer_pilot_support",
            "version": version,
            "pilot_name": pilot_name,
            "generated_at_utc": generated_at,
            "source_bundle_root": str(bundle_root),
            "state_db": _sanitize_mapping({"state_db": resolved_state_db}, bundle_root=bundle_root)["state_db"],
            "warnings": warnings,
            "artifacts": sorted(written_paths),
        }
        _write_json(staging_root / SUPPORT_MANIFEST_NAME, support_manifest)

        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            zip_timestamp = _zip_entry_timestamp()
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
    bundle_path = package_customer_pilot_support_bundle(
        bundle_root=_resolve_bundle_root(args.bundle_root),
        output_dir=Path(args.output_dir).resolve(),
        state_db=args.state_db,
        audit_event_limit=args.audit_event_limit,
        run_limit=args.run_limit,
    )
    print(f"customer pilot support bundle written: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
