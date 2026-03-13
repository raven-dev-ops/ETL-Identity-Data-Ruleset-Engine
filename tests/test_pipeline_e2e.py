import csv
import json
from pathlib import Path

from etl_identity_engine.cli import main


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_run_all_creates_expected_artifacts(tmp_path: Path) -> None:
    exit_code = main(
        [
            "run-all",
            "--base-dir",
            str(tmp_path),
            "--profile",
            "small",
            "--seed",
            "42",
        ]
    )
    assert exit_code == 0

    expected_files = [
        tmp_path / "data" / "synthetic_sources" / "person_source_a.csv",
        tmp_path / "data" / "synthetic_sources" / "person_source_b.csv",
        tmp_path / "data" / "synthetic_sources" / "conflict_annotations.csv",
        tmp_path / "data" / "synthetic_sources" / "incident_records.csv",
        tmp_path / "data" / "synthetic_sources" / "incident_person_links.csv",
        tmp_path / "data" / "synthetic_sources" / "address_history.csv",
        tmp_path / "data" / "synthetic_sources" / "generation_summary.json",
        tmp_path / "data" / "normalized" / "normalized_person_records.csv",
        tmp_path / "data" / "matches" / "candidate_scores.csv",
        tmp_path / "data" / "matches" / "entity_clusters.csv",
        tmp_path / "data" / "golden" / "golden_person_records.csv",
        tmp_path / "data" / "golden" / "source_to_golden_crosswalk.csv",
        tmp_path / "data" / "review_queue" / "manual_review_queue.csv",
        tmp_path / "data" / "exceptions" / "invalid_dobs.csv",
        tmp_path / "data" / "exceptions" / "malformed_phones.csv",
        tmp_path / "data" / "exceptions" / "normalization_failures.csv",
        tmp_path / "data" / "exceptions" / "run_report.md",
        tmp_path / "data" / "exceptions" / "run_summary.json",
    ]
    for path in expected_files:
        assert path.exists(), f"missing expected output: {path}"

    normalized_rows = _read_csv_rows(tmp_path / "data" / "normalized" / "normalized_person_records.csv")
    match_rows = _read_csv_rows(tmp_path / "data" / "matches" / "candidate_scores.csv")
    cluster_rows = _read_csv_rows(tmp_path / "data" / "matches" / "entity_clusters.csv")
    golden_rows = _read_csv_rows(tmp_path / "data" / "golden" / "golden_person_records.csv")
    crosswalk_rows = _read_csv_rows(tmp_path / "data" / "golden" / "source_to_golden_crosswalk.csv")
    review_queue_rows = _read_csv_rows(tmp_path / "data" / "review_queue" / "manual_review_queue.csv")
    summary = json.loads((tmp_path / "data" / "exceptions" / "run_summary.json").read_text(encoding="utf-8"))

    assert len(normalized_rows) == 48
    assert match_rows
    assert any(float(row["score"]) > 0.0 for row in match_rows)
    assert {"decision", "matched_fields", "reason_trace"} <= set(match_rows[0])
    assert len(cluster_rows) == len(normalized_rows)
    assert len(crosswalk_rows) == len(normalized_rows)
    assert len(golden_rows) == len({row["cluster_id"] for row in cluster_rows})
    assert len(golden_rows) >= 24
    assert len(golden_rows) <= len(normalized_rows)
    assert {"cluster_id", "first_name_source_record_id", "first_name_rule_name"} <= set(golden_rows[0])
    assert len({row["golden_id"] for row in crosswalk_rows}) == len(golden_rows)
    assert summary["candidate_pair_count"] == len(match_rows)
    assert summary["cluster_count"] == len(golden_rows)
    assert summary["golden_record_count"] == len(golden_rows)
    assert summary["review_queue_count"] == len(review_queue_rows)
    if review_queue_rows:
        assert {"reason_codes", "top_contributing_match_signals"} <= set(review_queue_rows[0])
