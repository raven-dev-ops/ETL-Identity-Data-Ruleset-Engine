from __future__ import annotations

import csv
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.service_api import create_service_app
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


def _create_persisted_manifest_run(tmp_path: Path) -> tuple[Path, str, SQLitePipelineStore]:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="service-api-001",
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
    return db_path, run_id, store


def test_service_api_exposes_run_golden_crosswalk_and_review_state(tmp_path: Path) -> None:
    db_path, run_id, store = _create_persisted_manifest_run(tmp_path)
    bundle = store.load_run_bundle(run_id)
    crosswalk_row = bundle.crosswalk_rows[0]
    review_row = bundle.review_rows[0]

    client = TestClient(create_service_app(db_path))

    health_response = client.get("/healthz")
    assert health_response.status_code == 200
    assert health_response.json()["status"] == "ok"

    latest_run_response = client.get("/api/v1/runs/latest")
    assert latest_run_response.status_code == 200
    assert latest_run_response.json()["run_id"] == run_id
    assert latest_run_response.json()["status"] == "completed"

    run_response = client.get(f"/api/v1/runs/{run_id}")
    assert run_response.status_code == 200
    assert run_response.json()["summary"]["run_context"]["input_mode"] == "manifest"

    crosswalk_response = client.get(
        f"/api/v1/runs/{run_id}/crosswalk/source-records/{crosswalk_row['source_record_id']}"
    )
    assert crosswalk_response.status_code == 200
    assert crosswalk_response.json() == crosswalk_row

    golden_response = client.get(
        f"/api/v1/runs/{run_id}/golden-records/{crosswalk_row['golden_id']}"
    )
    assert golden_response.status_code == 200
    assert golden_response.json()["golden_id"] == crosswalk_row["golden_id"]
    assert golden_response.json()["cluster_id"] == crosswalk_row["cluster_id"]

    review_list_response = client.get(f"/api/v1/runs/{run_id}/review-cases")
    assert review_list_response.status_code == 200
    assert review_list_response.json()[0]["review_id"] == review_row["review_id"]
    assert review_list_response.json()[0]["queue_status"] == "pending"

    review_detail_response = client.get(
        f"/api/v1/runs/{run_id}/review-cases/{review_row['review_id']}"
    )
    assert review_detail_response.status_code == 200
    assert review_detail_response.json()["left_id"] == review_row["left_id"]
    assert review_detail_response.json()["right_id"] == review_row["right_id"]


def test_service_api_validates_request_inputs_and_returns_not_found_for_missing_rows(
    tmp_path: Path,
) -> None:
    db_path, run_id, _store = _create_persisted_manifest_run(tmp_path)
    client = TestClient(create_service_app(db_path))

    invalid_status_response = client.get(
        f"/api/v1/runs/{run_id}/review-cases",
        params={"status": "not-a-valid-status"},
    )
    assert invalid_status_response.status_code == 422

    invalid_run_id_response = client.get("/api/v1/runs/not-a-run-id")
    assert invalid_run_id_response.status_code == 422

    missing_golden_response = client.get(f"/api/v1/runs/{run_id}/golden-records/G-99999")
    assert missing_golden_response.status_code == 404

    missing_crosswalk_response = client.get(
        f"/api/v1/runs/{run_id}/crosswalk/source-records/UNKNOWN-RECORD"
    )
    assert missing_crosswalk_response.status_code == 404

    with pytest.raises(FileNotFoundError, match="Persisted state database not found"):
        create_service_app(tmp_path / "missing.sqlite")
