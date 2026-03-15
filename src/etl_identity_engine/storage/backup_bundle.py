"""Portable persisted-state export and restore helpers."""

from __future__ import annotations

from collections.abc import Sequence
import json
from pathlib import Path
import shutil

from sqlalchemy import text

from etl_identity_engine.storage.migration_runner import current_state_store_revision, upgrade_state_store
from etl_identity_engine.storage.sqlite_store import PIPELINE_STATE_TABLES, SQLitePipelineStore
from etl_identity_engine.storage.state_store_target import resolve_state_store_target


STATE_BACKUP_MANIFEST_NAME = "state_backup_manifest.json"
TABLE_EXPORT_DIRNAME = "tables"
ATTACHMENTS_DIRNAME = "attachments"
STATE_EXPORT_DIRNAME = "state_export"
STATE_BACKUP_BUNDLE_TYPE = "state_backup"

TABLE_EXPORT_ORDERS: dict[str, str] = {
    "pipeline_runs": "run_id ASC",
    "run_checkpoints": "run_id ASC, stage_order ASC, checkpoint_id ASC",
    "export_job_runs": "export_run_id ASC",
    "audit_events": "occurred_at_utc ASC, audit_event_id ASC",
    "normalized_source_records": "run_id ASC, row_index ASC",
    "candidate_pairs": "run_id ASC, row_index ASC",
    "blocking_metrics": "run_id ASC, row_index ASC",
    "entity_clusters": "run_id ASC, row_index ASC",
    "golden_records": "run_id ASC, row_index ASC",
    "source_to_golden_crosswalk": "run_id ASC, row_index ASC",
    "review_cases": "run_id ASC, row_index ASC",
    "public_safety_incident_identity": "run_id ASC, row_index ASC",
    "public_safety_golden_activity": "run_id ASC, row_index ASC",
}


def export_state_backup(
    *,
    state_db: str | Path,
    destination_root: Path,
    include_paths: Sequence[Path] = (),
) -> dict[str, object]:
    destination_root.mkdir(parents=True, exist_ok=True)
    state_export_root = destination_root / STATE_EXPORT_DIRNAME
    tables_root = state_export_root / TABLE_EXPORT_DIRNAME
    attachments_root = destination_root / ATTACHMENTS_DIRNAME
    tables_root.mkdir(parents=True, exist_ok=True)

    store = SQLitePipelineStore(state_db)
    row_counts: dict[str, int] = {}
    try:
        with store.engine.connect() as connection:
            for table_name in PIPELINE_STATE_TABLES:
                order_by = TABLE_EXPORT_ORDERS[table_name]
                rows = [
                    dict(row)
                    for row in connection.execute(
                        text(f"SELECT * FROM {table_name} ORDER BY {order_by}")
                    ).mappings()
                ]
                row_counts[table_name] = len(rows)
                table_path = tables_root / f"{table_name}.jsonl"
                with table_path.open("w", encoding="utf-8", newline="\n") as handle:
                    for row in rows:
                        handle.write(json.dumps(row, sort_keys=True) + "\n")
    finally:
        store.engine.dispose()

    attachment_entries: list[dict[str, object]] = []
    for include_path in include_paths:
        resolved_path = include_path.resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"Included backup path not found: {resolved_path}")
        destination_path = attachments_root / resolved_path.name
        if destination_path.exists():
            raise ValueError(f"Duplicate attachment name in backup export: {resolved_path.name}")
        if resolved_path.is_dir():
            shutil.copytree(resolved_path, destination_path)
            kind = "directory"
        else:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(resolved_path, destination_path)
            kind = "file"
        attachment_entries.append(
            {
                "name": resolved_path.name,
                "kind": kind,
                "bundle_path": str(destination_path.relative_to(destination_root)).replace("\\", "/"),
                "source_path": str(resolved_path),
            }
        )

    target = resolve_state_store_target(state_db)
    manifest = {
        "bundle_type": STATE_BACKUP_BUNDLE_TYPE,
        "schema_revision": current_state_store_revision(target.raw_value),
        "source_state_store": target.display_name,
        "source_backend": target.backend,
        "tables": {table_name: {"row_count": row_counts[table_name]} for table_name in PIPELINE_STATE_TABLES},
        "attachments": attachment_entries,
    }
    manifest_path = state_export_root / STATE_BACKUP_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "manifest_path": str(manifest_path),
        "row_counts": row_counts,
        "attachments": attachment_entries,
    }


def restore_state_backup(
    *,
    source_root: Path,
    state_db: str | Path,
    attachments_output_root: Path | None = None,
    replace_existing: bool = False,
) -> dict[str, object]:
    source_root = source_root.resolve()
    manifest_path = source_root / STATE_EXPORT_DIRNAME / STATE_BACKUP_MANIFEST_NAME
    if not manifest_path.exists():
        raise FileNotFoundError(f"State backup manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    upgrade_state_store(state_db)
    store = SQLitePipelineStore(state_db)
    restored_row_counts: dict[str, int] = {}
    try:
        with store.engine.begin() as connection:
            existing_row_total = 0
            for table_name in PIPELINE_STATE_TABLES:
                existing_row_total += int(
                    connection.execute(text(f"SELECT COUNT(*) AS total FROM {table_name}")).scalar_one()
                )
            if existing_row_total and not replace_existing:
                raise ValueError(
                    "Target state store is not empty. Re-run with replace_existing=True to overwrite it."
                )

            for table_name in reversed(PIPELINE_STATE_TABLES):
                connection.execute(text(f"DELETE FROM {table_name}"))

            for table_name in PIPELINE_STATE_TABLES:
                table_path = source_root / STATE_EXPORT_DIRNAME / TABLE_EXPORT_DIRNAME / f"{table_name}.jsonl"
                if not table_path.exists():
                    raise FileNotFoundError(f"Backup bundle is missing table export: {table_path}")
                rows = [
                    json.loads(line)
                    for line in table_path.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]
                restored_row_counts[table_name] = len(rows)
                if not rows:
                    continue
                column_names = tuple(rows[0].keys())
                placeholders = ", ".join(f":{column_name}" for column_name in column_names)
                quoted_columns = ", ".join(f'"{column_name}"' for column_name in column_names)
                connection.execute(
                    text(f"INSERT INTO {table_name} ({quoted_columns}) VALUES ({placeholders})"),
                    rows,
                )
    finally:
        store.engine.dispose()

    restored_attachments: list[dict[str, object]] = []
    attachment_entries = manifest.get("attachments", [])
    if attachments_output_root is not None and isinstance(attachment_entries, list):
        attachments_output_root.mkdir(parents=True, exist_ok=True)
        for attachment in attachment_entries:
            if not isinstance(attachment, dict):
                continue
            bundle_path = source_root / str(attachment.get("bundle_path", "") or "")
            if not bundle_path.exists():
                raise FileNotFoundError(f"Backup bundle attachment not found: {bundle_path}")
            restored_path = attachments_output_root / str(attachment.get("name", bundle_path.name))
            if bundle_path.is_dir():
                shutil.copytree(bundle_path, restored_path, dirs_exist_ok=True)
            else:
                restored_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(bundle_path, restored_path)
            restored_attachments.append(
                {
                    "name": str(attachment.get("name", "")),
                    "restored_path": str(restored_path),
                }
            )

    return {
        "manifest_path": str(manifest_path),
        "restored_row_counts": restored_row_counts,
        "restored_attachments": restored_attachments,
    }
