from __future__ import annotations

import csv
import json
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.ingest.public_safety_conformance import check_public_safety_onboarding
from public_safety_regression_fixture import copy_fixture_tree, load_scenario_expectations


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_public_safety_regression_fixture_tree_passes_onboarding_check() -> None:
    summary = check_public_safety_onboarding(
        manifest_path=Path("fixtures/public_safety_regressions/manifest.yml"),
    )

    assert summary["status"] == "passed"
    assert summary["bundle_count"] == 0
    manifest = summary["manifest"]
    assert isinstance(manifest, dict)
    assert manifest["batch_id"] == "fixture-public-safety-regressions-001"
    assert manifest["source_bundle_count"] == 2


def test_public_safety_regression_fixture_run_locks_expected_matching_and_identity_outcomes(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "public_safety_regressions"
    manifest_path = copy_fixture_tree(fixture_root)
    base_dir = tmp_path / "run"
    state_db = tmp_path / "state" / "pipeline_state.sqlite"
    expectations = load_scenario_expectations()

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--manifest",
                str(manifest_path),
                "--state-db",
                str(state_db),
            ]
        )
        == 0
    )

    match_rows = _read_csv_rows(base_dir / "data" / "matches" / "candidate_scores.csv")
    crosswalk_rows = _read_csv_rows(base_dir / "data" / "golden" / "source_to_golden_crosswalk.csv")
    golden_rows = _read_csv_rows(base_dir / "data" / "golden" / "golden_person_records.csv")
    summary = json.loads((base_dir / "data" / "exceptions" / "run_summary.json").read_text(encoding="utf-8"))
    public_safety_summary = json.loads(
        (base_dir / "data" / "public_safety_demo" / "public_safety_demo_summary.json").read_text(encoding="utf-8")
    )

    assert len(match_rows) == expectations["expected_summary"]["candidate_pair_count"]
    assert summary["candidate_pair_count"] == expectations["expected_summary"]["candidate_pair_count"]
    assert summary["decision_counts"] == expectations["expected_summary"]["decision_counts"]
    assert summary["cluster_count"] == expectations["expected_summary"]["cluster_count"]
    assert summary["golden_record_count"] == expectations["expected_summary"]["golden_record_count"]
    assert summary["review_queue_count"] == expectations["expected_summary"]["review_queue_count"]

    for key, value in expectations["expected_summary"]["public_safety_activity"].items():
        assert public_safety_summary[key] == value

    matches_by_pair = {
        frozenset((row["left_id"], row["right_id"])): row
        for row in match_rows
    }
    crosswalk_by_record_id = {
        row["source_record_id"]: row
        for row in crosswalk_rows
    }

    for scenario in expectations["scenarios"]:
        pair_key = frozenset((scenario["left_id"], scenario["right_id"]))
        assert pair_key in matches_by_pair
        match_row = matches_by_pair[pair_key]

        assert float(match_row["score"]) == scenario["expected_score"]
        assert match_row["decision"] == scenario["expected_decision"]
        assert match_row["matched_fields"].split(";") == scenario["expected_matched_fields"]

        left_golden_id = crosswalk_by_record_id[scenario["left_id"]]["golden_id"]
        right_golden_id = crosswalk_by_record_id[scenario["right_id"]]["golden_id"]
        if scenario["expected_same_golden"]:
            assert left_golden_id == right_golden_id
        else:
            assert left_golden_id != right_golden_id

    assert len(golden_rows) == 5
    merged_golden = next(
        row
        for row in golden_rows
        if row["golden_id"] == crosswalk_by_record_id["A-SP-001"]["golden_id"]
    )
    assert merged_golden["source_record_count"] == "2"
    assert merged_golden["person_entity_id"] == "P-SP-001"
