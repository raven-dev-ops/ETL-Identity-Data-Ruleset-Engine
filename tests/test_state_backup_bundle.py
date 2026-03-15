from __future__ import annotations

import json
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.storage.backup_bundle import export_state_backup, restore_state_backup
from etl_identity_engine.storage.sqlite_store import PIPELINE_STATE_TABLES, SQLitePipelineStore


def _table_row_counts(db_path: Path) -> dict[str, int]:
    store = SQLitePipelineStore(db_path)
    try:
        with store.engine.connect() as connection:
            return {
                table_name: int(
                    connection.exec_driver_sql(f"SELECT COUNT(*) FROM {table_name}").scalar_one()
                )
                for table_name in PIPELINE_STATE_TABLES
            }
    finally:
        store.engine.dispose()


def test_export_and_restore_state_backup_round_trip(tmp_path: Path) -> None:
    source_db = tmp_path / "source" / "pipeline_state.sqlite"
    source_base_dir = tmp_path / "source-run"
    attachment_dir = tmp_path / "attachments" / "replay_bundle"
    attachment_dir.mkdir(parents=True, exist_ok=True)
    (attachment_dir / "metadata.json").write_text('{"bundle":"replay"}\n', encoding="utf-8")

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(source_base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--formats",
                "csv",
                "--state-db",
                str(source_db),
            ]
        )
        == 0
    )

    export_root = tmp_path / "backup-export"
    export_summary = export_state_backup(
        state_db=source_db,
        destination_root=export_root,
        include_paths=(attachment_dir,),
    )

    restored_db = tmp_path / "restored" / "pipeline_state.sqlite"
    restored_attachments_root = tmp_path / "restored-attachments"
    restore_summary = restore_state_backup(
        source_root=export_root,
        state_db=restored_db,
        attachments_output_root=restored_attachments_root,
    )

    assert export_summary["row_counts"] == restore_summary["restored_row_counts"]
    assert (restored_attachments_root / "replay_bundle" / "metadata.json").read_text(encoding="utf-8") == (
        '{"bundle":"replay"}\n'
    )

    source_counts = _table_row_counts(source_db)
    restored_counts = _table_row_counts(restored_db)
    assert source_counts == restored_counts

    source_store = SQLitePipelineStore(source_db)
    restored_store = SQLitePipelineStore(restored_db)
    try:
        source_run_id = source_store.latest_completed_run_id()
        restored_run_id = restored_store.latest_completed_run_id()
        assert source_run_id is not None
        assert source_run_id == restored_run_id
        assert restored_store.load_run_record(restored_run_id).summary == source_store.load_run_record(source_run_id).summary
    finally:
        source_store.engine.dispose()
        restored_store.engine.dispose()


def test_cli_backup_state_bundle_and_restore_state_bundle_round_trip(tmp_path: Path, capsys) -> None:
    source_db = tmp_path / "source" / "pipeline_state.sqlite"
    source_base_dir = tmp_path / "source-run"
    attachment_dir = tmp_path / "attachments" / "config_snapshot"
    attachment_dir.mkdir(parents=True, exist_ok=True)
    (attachment_dir / "runtime.json").write_text(json.dumps({"environment": "demo"}, indent=2), encoding="utf-8")
    passphrase_file = tmp_path / "backup-passphrase.txt"
    passphrase_file.write_text("bundle-passphrase\n", encoding="utf-8")

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(source_base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--formats",
                "csv",
                "--state-db",
                str(source_db),
            ]
        )
        == 0
    )
    capsys.readouterr()

    encrypted_bundle = tmp_path / "backup" / "state-backup.zip"
    assert (
        main(
            [
                "backup-state-bundle",
                "--state-db",
                str(source_db),
                "--output",
                str(encrypted_bundle),
                "--include-path",
                str(attachment_dir),
                "--passphrase-file",
                str(passphrase_file),
            ]
        )
        == 0
    )
    backup_payload = json.loads(capsys.readouterr().out)
    assert backup_payload["action"] == "created"
    assert Path(backup_payload["bundle_path"]) == encrypted_bundle.resolve()

    restored_db = tmp_path / "restored" / "pipeline_state.sqlite"
    restored_attachments_root = tmp_path / "restored-attachments"
    assert (
        main(
            [
                "restore-state-bundle",
                "--state-db",
                str(restored_db),
                "--bundle",
                str(encrypted_bundle),
                "--attachments-output-dir",
                str(restored_attachments_root),
                "--passphrase-file",
                str(passphrase_file),
            ]
        )
        == 0
    )
    restore_payload = json.loads(capsys.readouterr().out)

    assert restore_payload["action"] == "restored"
    assert restore_payload["bundle_type"] == "state_backup"
    assert (restored_attachments_root / "config_snapshot" / "runtime.json").exists()
    assert _table_row_counts(source_db) == _table_row_counts(restored_db)
