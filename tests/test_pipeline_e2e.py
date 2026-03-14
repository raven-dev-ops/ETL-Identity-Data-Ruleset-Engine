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
        tmp_path / "data" / "matches" / "blocking_metrics.csv",
        tmp_path / "data" / "matches" / "entity_clusters.csv",
        tmp_path / "data" / "golden" / "golden_person_records.csv",
        tmp_path / "data" / "golden" / "source_to_golden_crosswalk.csv",
        tmp_path / "data" / "public_safety_demo" / "incident_identity_view.csv",
        tmp_path / "data" / "public_safety_demo" / "golden_person_activity.csv",
        tmp_path / "data" / "public_safety_demo" / "public_safety_demo_dashboard.html",
        tmp_path / "data" / "public_safety_demo" / "public_safety_demo_report.md",
        tmp_path / "data" / "public_safety_demo" / "public_safety_demo_scenarios.json",
        tmp_path / "data" / "public_safety_demo" / "public_safety_demo_summary.json",
        tmp_path / "data" / "public_safety_demo" / "public_safety_demo_walkthrough.md",
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
    blocking_metrics_rows = _read_csv_rows(tmp_path / "data" / "matches" / "blocking_metrics.csv")
    cluster_rows = _read_csv_rows(tmp_path / "data" / "matches" / "entity_clusters.csv")
    golden_rows = _read_csv_rows(tmp_path / "data" / "golden" / "golden_person_records.csv")
    crosswalk_rows = _read_csv_rows(tmp_path / "data" / "golden" / "source_to_golden_crosswalk.csv")
    incident_identity_rows = _read_csv_rows(tmp_path / "data" / "public_safety_demo" / "incident_identity_view.csv")
    golden_activity_rows = _read_csv_rows(tmp_path / "data" / "public_safety_demo" / "golden_person_activity.csv")
    review_queue_rows = _read_csv_rows(tmp_path / "data" / "review_queue" / "manual_review_queue.csv")
    summary = json.loads((tmp_path / "data" / "exceptions" / "run_summary.json").read_text(encoding="utf-8"))
    report_text = (tmp_path / "data" / "exceptions" / "run_report.md").read_text(encoding="utf-8")
    public_safety_summary = json.loads(
        (tmp_path / "data" / "public_safety_demo" / "public_safety_demo_summary.json").read_text(encoding="utf-8")
    )
    public_safety_report_text = (
        tmp_path / "data" / "public_safety_demo" / "public_safety_demo_report.md"
    ).read_text(encoding="utf-8")
    public_safety_dashboard_text = (
        tmp_path / "data" / "public_safety_demo" / "public_safety_demo_dashboard.html"
    ).read_text(encoding="utf-8")
    public_safety_scenarios = json.loads(
        (tmp_path / "data" / "public_safety_demo" / "public_safety_demo_scenarios.json").read_text(encoding="utf-8")
    )
    public_safety_walkthrough_text = (
        tmp_path / "data" / "public_safety_demo" / "public_safety_demo_walkthrough.md"
    ).read_text(encoding="utf-8")

    assert len(normalized_rows) == 48
    assert match_rows
    assert blocking_metrics_rows
    assert any(float(row["score"]) > 0.0 for row in match_rows)
    assert {"decision", "matched_fields", "reason_trace"} <= set(match_rows[0])
    assert {
        "pass_name",
        "raw_candidate_pair_count",
        "new_candidate_pair_count",
        "overall_deduplicated_candidate_pair_count",
    } <= set(blocking_metrics_rows[0])
    assert int(blocking_metrics_rows[-1]["overall_deduplicated_candidate_pair_count"]) == len(match_rows)
    assert len(cluster_rows) == len(normalized_rows)
    assert len(crosswalk_rows) == len(normalized_rows)
    assert len(golden_rows) == len({row["cluster_id"] for row in cluster_rows})
    assert len(golden_rows) >= 24
    assert len(golden_rows) <= len(normalized_rows)
    assert {"cluster_id", "first_name_source_record_id", "first_name_rule_name"} <= set(golden_rows[0])
    assert len({row["golden_id"] for row in crosswalk_rows}) == len(golden_rows)
    assert incident_identity_rows
    assert golden_activity_rows
    assert all(row["incident_source_system"] in {"cad", "rms"} for row in incident_identity_rows)
    assert all(row["golden_id"] for row in incident_identity_rows)
    assert public_safety_summary["resolved_link_count"] == len(incident_identity_rows)
    assert public_safety_summary["unresolved_link_count"] == 0
    assert public_safety_summary["linked_golden_person_count"] == len(golden_activity_rows)
    assert public_safety_summary["cad_incident_count"] + public_safety_summary["rms_incident_count"] == public_safety_summary["incident_count"]
    assert public_safety_summary["demo_scenarios"] == public_safety_scenarios
    assert len(public_safety_scenarios) >= 2
    assert public_safety_report_text.startswith("# Public Safety Demo Report\n")
    assert "## Suggested Demo Scenarios" in public_safety_report_text
    assert "## What It Demonstrates" in public_safety_report_text
    assert "<title>Public Safety Identity Demo</title>" in public_safety_dashboard_text
    assert "Suggested Demo Scenarios" in public_safety_dashboard_text
    assert public_safety_walkthrough_text.startswith("# Public Safety Demo Walkthrough\n")
    assert "## Suggested Scenarios" in public_safety_walkthrough_text
    assert "Mock CAD/RMS Identity Demo" in public_safety_dashboard_text
    assert summary["candidate_pair_count"] == len(match_rows)
    assert summary["cluster_count"] == len(golden_rows)
    assert summary["golden_record_count"] == len(golden_rows)
    assert summary["review_queue_count"] == len(review_queue_rows)
    assert summary["before_after_completeness"]["name"]["after"] == summary["completeness"]["canonical_name_present"]
    assert summary["duplicate_reduction"]["after_record_count"] == len(golden_rows)
    assert summary["duplicate_reduction"]["records_consolidated"] == len(normalized_rows) - len(golden_rows)
    assert summary["performance"]["phase_metrics"]["normalize"]["output_record_count"] == len(normalized_rows)
    assert summary["performance"]["phase_metrics"]["match"]["candidate_pair_count"] == len(match_rows)
    assert "- Input file: `../normalized/normalized_person_records.csv`" in report_text
    assert str(tmp_path).replace("\\", "/") not in report_text
    if review_queue_rows:
        assert {"reason_codes", "top_contributing_match_signals"} <= set(review_queue_rows[0])


def test_standalone_golden_and_report_reuse_pipeline_artifacts(tmp_path: Path) -> None:
    assert main(["run-all", "--base-dir", str(tmp_path), "--profile", "small", "--seed", "42"]) == 0

    normalized_file = tmp_path / "data" / "normalized" / "normalized_person_records.csv"
    matches_file = tmp_path / "data" / "matches" / "candidate_scores.csv"
    clusters_file = tmp_path / "data" / "matches" / "entity_clusters.csv"
    rerun_clusters_file = tmp_path / "data" / "matches" / "entity_clusters_rerun.csv"
    rerun_review_queue_file = tmp_path / "data" / "review_queue" / "manual_review_queue_rerun.csv"
    rerun_golden_file = tmp_path / "data" / "golden" / "golden_person_records_rerun.csv"
    rerun_report_file = tmp_path / "data" / "exceptions" / "run_report_rerun.md"
    rerun_summary_file = tmp_path / "data" / "exceptions" / "run_summary.json"

    original_cluster_rows = _read_csv_rows(clusters_file)
    original_review_queue_rows = _read_csv_rows(tmp_path / "data" / "review_queue" / "manual_review_queue.csv")
    original_golden_rows = _read_csv_rows(tmp_path / "data" / "golden" / "golden_person_records.csv")
    original_summary = json.loads((tmp_path / "data" / "exceptions" / "run_summary.json").read_text(encoding="utf-8"))

    assert (
        main(
            [
                "cluster",
                "--input",
                str(normalized_file),
                "--matches",
                str(matches_file),
                "--output",
                str(rerun_clusters_file),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "review-queue",
                "--input",
                str(matches_file),
                "--output",
                str(rerun_review_queue_file),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "golden",
                "--input",
                str(normalized_file),
                "--clusters",
                str(rerun_clusters_file),
                "--output",
                str(rerun_golden_file),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "report",
                "--input",
                str(normalized_file),
                "--output",
                str(rerun_report_file),
            ]
        )
        == 0
    )

    rerun_cluster_rows = _read_csv_rows(rerun_clusters_file)
    rerun_review_queue_rows = _read_csv_rows(rerun_review_queue_file)
    rerun_golden_rows = _read_csv_rows(rerun_golden_file)
    rerun_summary = json.loads(rerun_summary_file.read_text(encoding="utf-8"))

    assert rerun_cluster_rows == original_cluster_rows
    assert rerun_review_queue_rows == original_review_queue_rows
    assert rerun_golden_rows == original_golden_rows
    assert rerun_summary == original_summary


def test_run_all_supports_parquet_only_generation_inputs(tmp_path: Path) -> None:
    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(tmp_path),
                "--profile",
                "small",
                "--seed",
                "42",
                "--formats",
                "parquet",
            ]
        )
        == 0
    )

    assert (tmp_path / "data" / "synthetic_sources" / "person_source_a.parquet").exists()
    assert not (tmp_path / "data" / "synthetic_sources" / "person_source_a.csv").exists()
    assert (tmp_path / "data" / "normalized" / "normalized_person_records.csv").exists()
    assert (tmp_path / "data" / "golden" / "golden_person_records.csv").exists()
    assert (tmp_path / "data" / "public_safety_demo" / "incident_identity_view.csv").exists()
    assert (tmp_path / "data" / "public_safety_demo" / "public_safety_demo_dashboard.html").exists()
