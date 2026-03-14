from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.output_contracts import DELIVERY_CONTRACT_NAME, DELIVERY_CONTRACT_VERSION
from etl_identity_engine.storage.sqlite_store import SQLitePipelineStore


def _write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet_rows(path: Path, rows: list[dict[str, str]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _person_row(
    *,
    source_record_id: str,
    person_entity_id: str,
    source_system: str,
    first_name: str,
    last_name: str,
    dob: str,
    address: str,
    phone: str,
) -> dict[str, str]:
    return {
        "source_record_id": source_record_id,
        "person_entity_id": person_entity_id,
        "source_system": source_system,
        "first_name": first_name,
        "last_name": last_name,
        "dob": dob,
        "address": address,
        "city": "Columbus",
        "state": "OH",
        "postal_code": "43004",
        "phone": phone,
        "updated_at": "2025-01-01T00:00:00Z",
        "is_conflict_variant": "false",
        "conflict_types": "",
    }


def _write_manifest(path: Path, *, batch_id: str, source_a_path: str, source_b_path: str) -> Path:
    required_columns = "\n".join(f"        - {column}" for column in PERSON_HEADERS)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""
manifest_version: "1.0"
entity_type: person
batch_id: {batch_id}
landing_zone:
  kind: local_filesystem
  base_path: ./landing
sources:
  - source_id: source_a
    path: {source_a_path}
    format: csv
    schema_version: person-v1
    required_columns:
{required_columns}
  - source_id: source_b
    path: {source_b_path}
    format: parquet
    schema_version: person-v1
    required_columns:
{required_columns}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _create_manifest_review_run(tmp_path: Path) -> tuple[Path, Path, SQLitePipelineStore, str]:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="operator-cli-001",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )
    source_a_rows = [
        _person_row(
            source_record_id="A-1",
            person_entity_id="P-1",
            source_system="source_a",
            first_name="John",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        )
    ]
    source_b_rows = [
        _person_row(
            source_record_id="B-1",
            person_entity_id="P-2",
            source_system="source_b",
            first_name="Jon",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        )
    ]
    _write_csv_rows(landing_dir / "agency_a.csv", source_a_rows)
    _write_parquet_rows(landing_dir / "agency_b.parquet", source_b_rows)

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--manifest",
                str(manifest_path),
                "--state-db",
                str(db_path),
                "--refresh-mode",
                "full",
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    run_id = store.latest_completed_run_id()
    assert run_id is not None
    return db_path, manifest_path, store, run_id


def _json_output(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    return json.loads(capsys.readouterr().out)


def test_apply_review_decision_and_replay_run_support_operator_workflow(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path, manifest_path, store, first_run_id = _create_manifest_review_run(tmp_path)
    first_review_case = store.list_review_cases(run_id=first_run_id)[0]
    capsys.readouterr()

    assert (
        main(
            [
                "apply-review-decision",
                "--state-db",
                str(db_path),
                "--run-id",
                first_run_id,
                "--review-id",
                first_review_case.review_id,
                "--decision",
                "approved",
                "--assigned-to",
                "analyst.one",
                "--notes",
                "Approved for replay",
            ]
        )
        == 0
    )
    updated_payload = _json_output(capsys)
    assert updated_payload["action"] == "updated"
    assert updated_payload["case"]["queue_status"] == "approved"
    assert updated_payload["case"]["assigned_to"] == "analyst.one"

    assert (
        main(
            [
                "apply-review-decision",
                "--state-db",
                str(db_path),
                "--run-id",
                first_run_id,
                "--review-id",
                first_review_case.review_id,
                "--decision",
                "approved",
                "--assigned-to",
                "analyst.one",
                "--notes",
                "Approved for replay",
            ]
        )
        == 0
    )
    noop_payload = _json_output(capsys)
    assert noop_payload["action"] == "noop"
    assert noop_payload["case"]["queue_status"] == "approved"

    _write_manifest(
        manifest_path,
        batch_id="operator-cli-002",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )

    replay_base_dir = tmp_path / "replay-run"
    assert (
        main(
            [
                "replay-run",
                "--state-db",
                str(db_path),
                "--run-id",
                first_run_id,
                "--base-dir",
                str(replay_base_dir),
                "--refresh-mode",
                "incremental",
            ]
        )
        == 0
    )
    replay_payload = _json_output(capsys)
    assert replay_payload["action"] == "replayed"
    result_run_id = str(replay_payload["result_run_id"])
    assert result_run_id != first_run_id
    assert replay_payload["refresh_mode"] == "incremental"

    replay_bundle = store.load_run_bundle(result_run_id)
    assert replay_bundle.candidate_pairs[0]["decision"] == "auto_merge"
    assert "review_case_approved_override" in replay_bundle.candidate_pairs[0]["reason_trace"]
    assert len(replay_bundle.golden_rows) == 1
    assert replay_bundle.review_rows[0]["queue_status"] == "approved"

    second_replay_base_dir = tmp_path / "replay-run-again"
    assert (
        main(
            [
                "replay-run",
                "--state-db",
                str(db_path),
                "--run-id",
                result_run_id,
                "--base-dir",
                str(second_replay_base_dir),
                "--refresh-mode",
                "incremental",
            ]
        )
        == 0
    )
    replay_noop_payload = _json_output(capsys)
    assert replay_noop_payload["action"] == "reused_completed_run"
    assert replay_noop_payload["result_run_id"] == result_run_id


def test_publish_run_returns_json_and_reuses_existing_snapshot(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path, _manifest_path, store, run_id = _create_manifest_review_run(tmp_path)
    publish_root = tmp_path / "published"
    capsys.readouterr()

    assert (
        main(
            [
                "publish-run",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--output-dir",
                str(publish_root),
            ]
        )
        == 0
    )
    published_payload = _json_output(capsys)
    assert published_payload["action"] == "published"
    snapshot_dir = Path(str(published_payload["snapshot_dir"]))
    assert snapshot_dir == (
        publish_root / DELIVERY_CONTRACT_NAME / DELIVERY_CONTRACT_VERSION / "snapshots" / run_id
    )
    assert snapshot_dir.exists()

    assert (
        main(
            [
                "publish-run",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--output-dir",
                str(publish_root),
            ]
        )
        == 0
    )
    reused_payload = _json_output(capsys)
    assert reused_payload["action"] == "reused_snapshot"


def test_replay_run_rejects_non_manifest_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline.sqlite"
    base_dir = tmp_path / "synthetic"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    run_id = store.latest_completed_run_id()
    assert run_id is not None

    with pytest.raises(ValueError, match="replay-run currently supports persisted manifest runs only"):
        main(
            [
                "replay-run",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
            ]
        )
