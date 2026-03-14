from __future__ import annotations

import csv
import json
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.runtime_config import load_export_job_configs
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


def _create_manifest_run(tmp_path: Path) -> tuple[Path, str]:
    db_path = tmp_path / "state" / "pipeline.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="export-job-001",
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
            person_entity_id="P-1",
            source_system="source_b",
            first_name="John",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main Street",
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
    return db_path, run_id


def test_export_job_list_and_run_track_auditable_exports(
    tmp_path: Path,
    capsys,
) -> None:
    db_path, run_id = _create_manifest_run(tmp_path)
    config_dir = Path(__file__).resolve().parents[1] / "config"
    capsys.readouterr()

    assert main(["export-job-list", "--config-dir", str(config_dir)]) == 0
    listed_jobs = json.loads(capsys.readouterr().out)
    assert [job["name"] for job in listed_jobs] == [
        "warehouse_identity_snapshot",
        "data_product_identity_snapshot",
    ]
    assert listed_jobs[0]["consumer"] == "warehouse"

    assert (
        main(
            [
                "export-job-run",
                "--config-dir",
                str(config_dir),
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--job-name",
                "warehouse_identity_snapshot",
            ]
        )
        == 0
    )
    export_payload = json.loads(capsys.readouterr().out)
    assert export_payload["action"] == "exported"
    assert export_payload["job"]["name"] == "warehouse_identity_snapshot"
    assert export_payload["export_run"]["status"] == "completed"
    assert export_payload["export_run"]["source_run_id"] == run_id
    assert Path(export_payload["export_run"]["snapshot_dir"]).exists()
    assert export_payload["export_run"]["row_counts"]["golden_records"] == 1
    assert export_payload["export_run"]["row_counts"]["source_to_golden_crosswalk"] == 2

    assert (
        main(
            [
                "export-job-run",
                "--config-dir",
                str(config_dir),
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--job-name",
                "warehouse_identity_snapshot",
            ]
        )
        == 0
    )
    reused_payload = json.loads(capsys.readouterr().out)
    assert reused_payload["action"] == "reused_completed_export"
    assert reused_payload["export_run"]["export_run_id"] == export_payload["export_run"]["export_run_id"]

    assert (
        main(
            [
                "export-job-history",
                "--state-db",
                str(db_path),
                "--job-name",
                "warehouse_identity_snapshot",
            ]
        )
        == 0
    )
    history_payload = json.loads(capsys.readouterr().out)
    assert len(history_payload) == 1
    assert history_payload[0]["job_name"] == "warehouse_identity_snapshot"
    assert history_payload[0]["status"] == "completed"


def test_load_export_job_configs_resolves_documented_default_catalog() -> None:
    config_dir = Path(__file__).resolve().parents[1] / "config"

    jobs = load_export_job_configs(config_dir)

    assert set(jobs) == {"warehouse_identity_snapshot", "data_product_identity_snapshot"}
    assert jobs["warehouse_identity_snapshot"].consumer == "warehouse"
    assert jobs["data_product_identity_snapshot"].consumer == "data_product"
    assert jobs["warehouse_identity_snapshot"].output_root.name == "person_identity"
