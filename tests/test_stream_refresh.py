from __future__ import annotations

import json
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.storage.sqlite_store import SQLitePipelineStore
from etl_identity_engine.streaming import write_stream_events_jsonl


def _json_output(capsys) -> dict[str, object]:
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _raw_person_row(row: dict[str, str]) -> dict[str, str]:
    return {column: str(row.get(column, "") or "") for column in PERSON_HEADERS}


def test_stream_refresh_applies_ordered_events_and_persists_audit_trail(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "state" / "pipeline.sqlite"
    seed_base = tmp_path / "seed"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(seed_base),
                "--profile",
                "small",
                "--seed",
                "42",
                "--formats",
                "csv",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    source_run_id = store.latest_completed_run_id()
    assert source_run_id is not None
    source_bundle = store.load_run_bundle(source_run_id)
    source_rows = sorted(source_bundle.normalized_rows, key=lambda row: row["source_record_id"])
    update_row = _raw_person_row(source_rows[0])
    delete_row = _raw_person_row(source_rows[1])

    update_row["phone"] = "555-222-9999"
    update_row["updated_at"] = "2026-03-14T10:00:00Z"
    inserted_row = {
        "source_record_id": "STREAM-NEW-001",
        "person_entity_id": "STREAM-P-001",
        "source_system": update_row["source_system"],
        "first_name": "Avery",
        "last_name": "Stream",
        "dob": "1991-04-05",
        "address": "501 Stream Ave",
        "city": "Columbus",
        "state": "OH",
        "postal_code": "43004",
        "phone": "555-333-1212",
        "updated_at": "2026-03-14T10:05:00Z",
        "is_conflict_variant": "false",
        "conflict_types": "",
    }

    events_path = tmp_path / "events" / "batch_001.jsonl"
    write_stream_events_jsonl(
        events_path,
        [
            {
                "event_id": "ops-stream-000001",
                "stream_id": "ops_stream",
                "sequence": 1,
                "operation": "upsert",
                "occurred_at_utc": "2026-03-14T10:00:00Z",
                "source_record_id": update_row["source_record_id"],
                "source_system": update_row["source_system"],
                "record": update_row,
            },
            {
                "event_id": "ops-stream-000002",
                "stream_id": "ops_stream",
                "sequence": 2,
                "operation": "upsert",
                "occurred_at_utc": "2026-03-14T10:05:00Z",
                "source_record_id": inserted_row["source_record_id"],
                "source_system": inserted_row["source_system"],
                "record": inserted_row,
            },
            {
                "event_id": "ops-stream-000003",
                "stream_id": "ops_stream",
                "sequence": 3,
                "operation": "delete",
                "occurred_at_utc": "2026-03-14T10:10:00Z",
                "source_record_id": delete_row["source_record_id"],
                "source_system": delete_row["source_system"],
            },
        ],
    )

    stream_base = tmp_path / "stream"
    capsys.readouterr()
    assert (
        main(
            [
                "stream-refresh",
                "--base-dir",
                str(stream_base),
                "--state-db",
                str(db_path),
                "--source-run-id",
                source_run_id,
                "--events",
                str(events_path),
                "--stream-id",
                "ops_stream",
            ]
        )
        == 0
    )
    payload = _json_output(capsys)
    assert payload["action"] == "stream_refreshed"

    result_run_id = str(payload["run_id"])
    result_run = store.load_run_record(result_run_id)
    assert result_run.input_mode == "event_stream"
    assert result_run.summary["refresh"]["predecessor_run_id"] == source_run_id
    assert result_run.summary["stream"]["event_count"] == 3
    assert result_run.summary["stream"]["inserted_record_count"] == 1
    assert result_run.summary["stream"]["deleted_existing_record_count"] == 1
    assert result_run.summary["stream"]["updated_record_count"] == 1

    snapshot_path = stream_base / "data" / "events" / "stream_events.jsonl"
    assert snapshot_path.exists()
    assert snapshot_path.read_text(encoding="utf-8") == events_path.read_text(encoding="utf-8")

    result_bundle = store.load_run_bundle(result_run_id)
    result_ids = {row["source_record_id"] for row in result_bundle.normalized_rows}
    assert inserted_row["source_record_id"] in result_ids
    assert delete_row["source_record_id"] not in result_ids
    updated_row = next(row for row in result_bundle.normalized_rows if row["source_record_id"] == update_row["source_record_id"])
    assert updated_row["canonical_phone"] == "5552229999"

    audit_events = store.list_audit_events(run_id=result_run_id, action="stream_refresh", limit=5)
    assert audit_events
    assert audit_events[0].status == "succeeded"

    assert (
        main(
            [
                "stream-refresh",
                "--base-dir",
                str(tmp_path / "stream-rerun"),
                "--state-db",
                str(db_path),
                "--source-run-id",
                source_run_id,
                "--events",
                str(events_path),
                "--stream-id",
                "ops_stream",
            ]
        )
        == 0
    )
    reused_payload = _json_output(capsys)
    assert reused_payload["action"] == "reused_completed_run"
    assert reused_payload["run_id"] == result_run_id
