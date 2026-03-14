import csv
import json
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.output_contracts import (
    DELIVERY_ARTIFACT_HEADERS,
    DELIVERY_CONTRACT_NAME,
    DELIVERY_CONTRACT_VERSION,
    DELIVERY_CURRENT_POINTER_KEYS,
    DELIVERY_MANIFEST_KEYS,
    MATCH_DECISIONS,
    PIPELINE_ARTIFACT_PATHS,
    PIPELINE_CSV_ARTIFACT_HEADERS,
    SUMMARY_BEFORE_AFTER_FIELDS,
    SUMMARY_BEFORE_AFTER_KEYS,
    SUMMARY_COMPLETENESS_KEYS,
    SUMMARY_DUPLICATE_REDUCTION_KEYS,
    SUMMARY_PERFORMANCE_KEYS,
    SUMMARY_PHASE_METRIC_KEYS,
    SUMMARY_EXCEPTION_KEYS,
    SUMMARY_TOP_LEVEL_KEYS,
)


def _read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _is_int_like(value: str) -> bool:
    try:
        int(value)
    except ValueError:
        return False
    return True


def _is_float_like(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def test_run_all_outputs_follow_documented_contracts(tmp_path: Path) -> None:
    assert main(["run-all", "--base-dir", str(tmp_path), "--profile", "small"]) == 0

    expected_paths = {tmp_path / relative_path for relative_path in PIPELINE_ARTIFACT_PATHS}
    assert expected_paths <= {path for path in tmp_path.rglob("*") if path.is_file()}

    for relative_path, expected_header in PIPELINE_CSV_ARTIFACT_HEADERS.items():
        artifact_path = tmp_path / relative_path
        assert _read_header(artifact_path) == list(expected_header)

        for row in _read_rows(artifact_path):
            if "score" in row:
                assert _is_float_like(row["score"])
            if "decision" in row:
                assert row["decision"] in MATCH_DECISIONS
            if "queue_status" in row:
                assert row["queue_status"] == "pending"
            for numeric_field in (
                "raw_candidate_pair_count",
                "new_candidate_pair_count",
                "cumulative_candidate_pair_count",
                "overall_deduplicated_candidate_pair_count",
                "source_record_count",
            ):
                if numeric_field in row:
                    assert _is_int_like(row[numeric_field])

    summary_path = tmp_path / "data" / "exceptions" / "run_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert set(SUMMARY_TOP_LEVEL_KEYS) <= set(summary)
    assert isinstance(summary["total_records"], int)
    assert isinstance(summary["candidate_pair_count"], int)
    assert isinstance(summary["cluster_count"], int)
    assert isinstance(summary["golden_record_count"], int)
    assert isinstance(summary["review_queue_count"], int)
    assert all(isinstance(key, str) and isinstance(value, int) for key, value in summary["missing_field_counts"].items())
    assert set(summary["exception_counts"]) == set(SUMMARY_EXCEPTION_KEYS)
    assert all(isinstance(value, int) for value in summary["exception_counts"].values())
    assert set(summary["decision_counts"]) == set(MATCH_DECISIONS)
    assert all(isinstance(value, int) for value in summary["decision_counts"].values())
    assert set(summary["completeness"]) == set(SUMMARY_COMPLETENESS_KEYS)
    assert all(isinstance(value, int) for value in summary["completeness"].values())
    assert set(summary["before_after_completeness"]) == set(SUMMARY_BEFORE_AFTER_FIELDS)
    for field_name in SUMMARY_BEFORE_AFTER_FIELDS:
        field_metrics = summary["before_after_completeness"][field_name]
        assert set(field_metrics) == set(SUMMARY_BEFORE_AFTER_KEYS)
        assert all(isinstance(value, int) for value in field_metrics.values())
    assert set(summary["duplicate_reduction"]) == set(SUMMARY_DUPLICATE_REDUCTION_KEYS)
    assert isinstance(summary["duplicate_reduction"]["before_record_count"], int)
    assert isinstance(summary["duplicate_reduction"]["after_record_count"], int)
    assert isinstance(summary["duplicate_reduction"]["records_consolidated"], int)
    assert isinstance(summary["duplicate_reduction"]["reduction_ratio"], float)
    assert set(summary["performance"]) == set(SUMMARY_PERFORMANCE_KEYS)
    assert isinstance(summary["performance"]["total_duration_seconds"], float)
    assert isinstance(summary["performance"]["phase_metrics"], dict)
    for metrics in summary["performance"]["phase_metrics"].values():
        assert set(metrics) == set(SUMMARY_PHASE_METRIC_KEYS)

    report_path = tmp_path / "data" / "exceptions" / "run_report.md"
    report_text = report_path.read_text(encoding="utf-8")
    assert report_text.startswith("# Pipeline Report\n")
    assert "## Performance" in report_text
    assert "## Duplicate Reduction" in report_text


def test_delivery_contract_constants_are_versioned_and_complete() -> None:
    assert DELIVERY_CONTRACT_NAME == "golden_crosswalk_snapshot"
    assert DELIVERY_CONTRACT_VERSION == "v1"
    assert set(DELIVERY_ARTIFACT_HEADERS) == {
        Path("golden_person_records.csv"),
        Path("source_to_golden_crosswalk.csv"),
    }
    assert set(DELIVERY_MANIFEST_KEYS) == {
        "contract_name",
        "contract_version",
        "snapshot_id",
        "published_at_utc",
        "run_id",
        "state_db",
        "source_run",
        "row_counts",
        "artifacts",
    }
    assert set(DELIVERY_CURRENT_POINTER_KEYS) == {
        "contract_name",
        "contract_version",
        "snapshot_id",
        "run_id",
        "published_at_utc",
        "relative_snapshot_path",
        "relative_manifest_path",
    }
