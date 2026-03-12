from pathlib import Path

from etl_identity_engine.cli import main


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
        tmp_path / "data" / "golden" / "golden_person_records.csv",
        tmp_path / "data" / "exceptions" / "run_report.md",
    ]
    for path in expected_files:
        assert path.exists(), f"missing expected output: {path}"
