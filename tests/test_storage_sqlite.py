from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.storage.sqlite_store import (
    PIPELINE_STATE_TABLES,
    SQLitePipelineStore,
    bootstrap_sqlite_store,
)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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
