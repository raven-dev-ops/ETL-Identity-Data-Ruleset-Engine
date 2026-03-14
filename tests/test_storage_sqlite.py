from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.storage.sqlite_store import (
    PIPELINE_STATE_TABLES,
    SQLitePipelineStore,
    bootstrap_sqlite_store,
)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_pipeline_runs(db_path: Path) -> list[sqlite3.Row]:
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        return connection.execute(
            """
            SELECT run_id, run_key, attempt_number, batch_id, status, started_at_utc, finished_at_utc, failure_detail
            FROM pipeline_runs
            ORDER BY attempt_number ASC, run_id ASC
            """
        ).fetchall()


def _write_config_copy(target_dir: Path) -> None:
    source_dir = Path(__file__).resolve().parents[1] / "config"
    target_dir.mkdir(parents=True, exist_ok=True)
    for source_path in source_dir.glob("*.yml"):
        (target_dir / source_path.name).write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")


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


def test_bootstrap_sqlite_store_creates_expected_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"

    bootstrap_sqlite_store(db_path)

    with sqlite3.connect(db_path) as connection:
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert set(PIPELINE_STATE_TABLES) <= names


def test_run_all_persists_and_reload_state_from_sqlite(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"

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
    run_id = store.latest_run_id()
    assert run_id is not None

    bundle = store.load_run_bundle(run_id)
    summary = json.loads((base_dir / "data" / "exceptions" / "run_summary.json").read_text(encoding="utf-8"))

    assert bundle.run.status == "completed"
    assert bundle.run.input_mode == "synthetic"
    assert bundle.run.batch_id == "synthetic:small:42"
    assert bundle.run.total_records == summary["total_records"]
    assert bundle.run.candidate_pair_count == summary["candidate_pair_count"]
    assert bundle.run.cluster_count == summary["cluster_count"]
    assert bundle.run.golden_record_count == summary["golden_record_count"]
    assert bundle.run.review_queue_count == summary["review_queue_count"]
    assert bundle.run.summary == summary

    assert bundle.normalized_rows == _read_csv_rows(
        base_dir / "data" / "normalized" / "normalized_person_records.csv"
    )
    assert bundle.candidate_pairs == _read_csv_rows(
        base_dir / "data" / "matches" / "candidate_scores.csv"
    )
    assert bundle.blocking_metrics_rows == _read_csv_rows(
        base_dir / "data" / "matches" / "blocking_metrics.csv"
    )
    assert bundle.cluster_rows == _read_csv_rows(
        base_dir / "data" / "matches" / "entity_clusters.csv"
    )
    assert bundle.golden_rows == _read_csv_rows(
        base_dir / "data" / "golden" / "golden_person_records.csv"
    )
    assert bundle.crosswalk_rows == _read_csv_rows(
        base_dir / "data" / "golden" / "source_to_golden_crosswalk.csv"
    )
    assert bundle.review_rows == _read_csv_rows(
        base_dir / "data" / "review_queue" / "manual_review_queue.csv"
    )

    reloaded_report = tmp_path / "reloaded" / "run_report.md"
    assert (
        main(
            [
                "report",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--output",
                str(reloaded_report),
            ]
        )
        == 0
    )

    reloaded_summary = json.loads(
        reloaded_report.with_name("run_summary.json").read_text(encoding="utf-8")
    )
    reloaded_report_text = reloaded_report.read_text(encoding="utf-8")

    assert reloaded_summary == summary
    assert f"state-db://{db_path.name}?run_id={run_id}" in reloaded_report_text


def test_run_all_reuses_completed_run_without_duplicating_persisted_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"

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

    first_store = SQLitePipelineStore(db_path)
    first_run_id = first_store.latest_run_id()
    assert first_run_id is not None

    # Remove emitted artifacts to prove the second invocation restores them from persisted state.
    for child in (base_dir / "data").iterdir():
        if child.is_dir():
            for nested in child.rglob("*"):
                if nested.is_file():
                    nested.unlink()
            for nested in sorted(child.rglob("*"), reverse=True):
                if nested.is_dir():
                    nested.rmdir()
            child.rmdir()

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

    run_rows = _read_pipeline_runs(db_path)
    assert len(run_rows) == 1
    assert run_rows[0]["run_id"] == first_run_id
    assert run_rows[0]["status"] == "completed"
    assert (base_dir / "data" / "normalized" / "normalized_person_records.csv").exists()
    assert (base_dir / "data" / "exceptions" / "run_report.md").exists()


def test_run_all_records_failed_attempt_and_allows_clean_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    config_dir = tmp_path / "config"
    _write_config_copy(config_dir)

    (config_dir / "thresholds.yml").write_text(
        """
not_thresholds:
  auto_merge: 0.95
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"thresholds\.yml: top-level config contains unsupported keys: not_thresholds"):
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
                "--config-dir",
                str(config_dir),
            ]
        )

    failed_runs = _read_pipeline_runs(db_path)
    assert len(failed_runs) == 1
    assert failed_runs[0]["status"] == "failed"
    assert "thresholds.yml" in str(failed_runs[0]["failure_detail"])
    assert failed_runs[0]["attempt_number"] == 1

    _write_config_copy(config_dir)

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
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )

    run_rows = _read_pipeline_runs(db_path)
    assert len(run_rows) == 2
    assert run_rows[0]["status"] == "failed"
    assert run_rows[1]["status"] == "completed"
    assert run_rows[0]["run_key"] == run_rows[1]["run_key"]
    assert run_rows[1]["attempt_number"] == 2
    assert run_rows[1]["failure_detail"] in (None, "")


def test_incremental_manifest_refresh_reuses_unaffected_entities(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="inbound-2026-03-13",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )

    batch_one_source_a = [
        _person_row(
            source_record_id="A-1",
            person_entity_id="P-1",
            source_system="source_a",
            first_name="John",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        ),
        _person_row(
            source_record_id="A-2",
            person_entity_id="P-2",
            source_system="source_a",
            first_name="Jane",
            last_name="Doe",
            dob="1990-05-20",
            address="20 Oak St",
            phone="5552222222",
        ),
    ]
    batch_one_source_b = [
        _person_row(
            source_record_id="B-1",
            person_entity_id="P-1",
            source_system="source_b",
            first_name="Jon",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main Street",
            phone="5551111111",
        ),
        _person_row(
            source_record_id="B-2",
            person_entity_id="P-2",
            source_system="source_b",
            first_name="Jane",
            last_name="Doe",
            dob="1990-05-20",
            address="20 Oak Street Apt 2",
            phone="5552222222",
        ),
    ]
    _write_csv_rows(landing_dir / "agency_a.csv", batch_one_source_a)
    _write_parquet_rows(landing_dir / "agency_b.parquet", batch_one_source_b)

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
    first_run_id = store.latest_run_id()
    assert first_run_id is not None
    first_bundle = store.load_run_bundle(first_run_id)
    first_crosswalk_by_record = {
        row["source_record_id"]: row["golden_id"] for row in first_bundle.crosswalk_rows
    }
    first_cluster_by_record = {
        row["source_record_id"]: row["cluster_id"] for row in first_bundle.cluster_rows
    }
    first_golden_by_id = {
        row["golden_id"]: row for row in first_bundle.golden_rows
    }

    _write_manifest(
        manifest_path,
        batch_id="inbound-2026-03-14",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )
    batch_two_source_a = [dict(row) for row in batch_one_source_a]
    batch_two_source_a[1]["address"] = "99 Elm Street"
    batch_two_source_b = [dict(row) for row in batch_one_source_b]
    _write_csv_rows(landing_dir / "agency_a.csv", batch_two_source_a)
    _write_parquet_rows(landing_dir / "agency_b.parquet", batch_two_source_b)

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
                "incremental",
            ]
        )
        == 0
    )

    second_run_id = store.latest_run_id()
    assert second_run_id is not None
    assert second_run_id != first_run_id

    second_bundle = store.load_run_bundle(second_run_id)
    refresh = second_bundle.run.summary["refresh"]
    run_context = second_bundle.run.summary["run_context"]
    second_crosswalk_by_record = {
        row["source_record_id"]: row["golden_id"] for row in second_bundle.crosswalk_rows
    }
    second_cluster_by_record = {
        row["source_record_id"]: row["cluster_id"] for row in second_bundle.cluster_rows
    }
    second_golden_by_id = {
        row["golden_id"]: row for row in second_bundle.golden_rows
    }

    assert run_context["refresh_mode"] == "incremental"
    assert refresh["mode"] == "incremental"
    assert refresh["fallback_to_full"] is False
    assert refresh["predecessor_run_id"] == first_run_id
    assert refresh["changed_record_count"] == 1
    assert refresh["inserted_record_count"] == 0
    assert refresh["removed_record_count"] == 0
    assert refresh["affected_record_count"] == 2
    assert refresh["reused_record_count"] == 2
    assert refresh["recalculated_candidate_pair_count"] == 1
    assert refresh["reused_candidate_pair_count"] == 1
    assert refresh["recalculated_cluster_count"] == 1
    assert refresh["reused_cluster_count"] == 2

    assert second_cluster_by_record["A-1"] == first_cluster_by_record["A-1"]
    assert second_cluster_by_record["B-1"] == first_cluster_by_record["B-1"]
    assert second_crosswalk_by_record["A-1"] == first_crosswalk_by_record["A-1"]
    assert second_crosswalk_by_record["B-1"] == first_crosswalk_by_record["B-1"]
    assert (
        second_golden_by_id[second_crosswalk_by_record["A-1"]]["address"]
        == first_golden_by_id[first_crosswalk_by_record["A-1"]]["address"]
    )
    assert second_golden_by_id[second_crosswalk_by_record["A-2"]]["address"] == "99 Elm Street"
