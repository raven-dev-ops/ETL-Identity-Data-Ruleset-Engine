import csv
import json
import shutil
from pathlib import Path

from etl_identity_engine.cli import main


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_public_safety_demo_command_rebuilds_demo_outputs(tmp_path: Path) -> None:
    assert main(["run-all", "--base-dir", str(tmp_path), "--profile", "small", "--seed", "42"]) == 0

    output_dir = tmp_path / "data" / "public_safety_demo"
    original_view_rows = _read_csv_rows(output_dir / "incident_identity_view.csv")
    original_activity_rows = _read_csv_rows(output_dir / "golden_person_activity.csv")
    original_summary = json.loads((output_dir / "public_safety_demo_summary.json").read_text(encoding="utf-8"))
    original_dashboard = (output_dir / "public_safety_demo_dashboard.html").read_text(encoding="utf-8")
    original_scenarios = json.loads((output_dir / "public_safety_demo_scenarios.json").read_text(encoding="utf-8"))
    original_walkthrough = (output_dir / "public_safety_demo_walkthrough.md").read_text(encoding="utf-8")

    shutil.rmtree(output_dir)

    assert main(["public-safety-demo", "--base-dir", str(tmp_path)]) == 0

    rebuilt_view_rows = _read_csv_rows(output_dir / "incident_identity_view.csv")
    rebuilt_activity_rows = _read_csv_rows(output_dir / "golden_person_activity.csv")
    rebuilt_summary = json.loads((output_dir / "public_safety_demo_summary.json").read_text(encoding="utf-8"))
    rebuilt_dashboard = (output_dir / "public_safety_demo_dashboard.html").read_text(encoding="utf-8")
    rebuilt_scenarios = json.loads((output_dir / "public_safety_demo_scenarios.json").read_text(encoding="utf-8"))
    rebuilt_walkthrough = (output_dir / "public_safety_demo_walkthrough.md").read_text(encoding="utf-8")

    assert rebuilt_view_rows == original_view_rows
    assert rebuilt_activity_rows == original_activity_rows
    assert rebuilt_summary == original_summary
    assert rebuilt_dashboard == original_dashboard
    assert rebuilt_scenarios == original_scenarios
    assert rebuilt_walkthrough == original_walkthrough
    assert "<title>Public Safety Identity Demo</title>" in rebuilt_dashboard
    assert rebuilt_summary["demo_scenarios"] == rebuilt_scenarios
