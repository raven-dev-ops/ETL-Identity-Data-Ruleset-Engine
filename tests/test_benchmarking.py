import json
import shutil
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.runtime_config import default_config_dir


def test_benchmark_run_writes_summary_and_report(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    shutil.copytree(default_config_dir(), config_dir)
    (config_dir / "benchmark_fixtures.yml").write_text(
        """
benchmark_fixtures:
  - name: tiny
    description: Tiny benchmark fixture for integration coverage.
    profile: small
    person_count: 48
    duplicate_rate: 0.25
    seed: 42
    formats:
      - csv
    capacity_targets:
      single_host_container:
        max_total_duration_seconds: 120.0
        min_normalize_records_per_second: 0.0
        min_match_candidate_pairs_per_second: 0.0
""".strip()
        + "\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "benchmarks"
    assert (
        main(
            [
                "benchmark-run",
                "--fixture",
                "tiny",
                "--output-dir",
                str(output_dir),
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )

    summary_path = output_dir / "tiny" / "benchmark_summary.json"
    report_path = output_dir / "tiny" / "benchmark_report.md"
    run_summary_path = output_dir / "tiny" / "run_artifacts" / "data" / "exceptions" / "run_summary.json"

    assert summary_path.exists()
    assert report_path.exists()
    assert run_summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    report_text = report_path.read_text(encoding="utf-8")

    assert summary["fixture"]["name"] == "tiny"
    assert summary["capacity_assertions"]["status"] == "passed"
    assert summary["run_summary"]["performance"]["phase_metrics"]["normalize"]["output_record_count"] == 96
    assert "## Capacity Assertions" in report_text
    assert "`match`:" in report_text
