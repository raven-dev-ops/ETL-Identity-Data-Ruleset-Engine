from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.storage.sqlite_store import (
    PIPELINE_STATE_TABLES,
    SQLitePipelineStore,
    bootstrap_sqlite_store,
)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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
