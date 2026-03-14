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


def test_benchmark_run_supports_continuous_ingest_fixtures(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    shutil.copytree(default_config_dir(), config_dir)
    (config_dir / "benchmark_fixtures.yml").write_text(
        """
benchmark_fixtures:
  - name: stream_tiny
    description: Tiny continuous-ingest benchmark fixture.
    mode: event_stream
    profile: small
    person_count: 24
    duplicate_rate: 0.25
    seed: 42
    formats:
      - csv
    stream_batch_count: 2
    stream_events_per_batch: 3
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
                "stream_tiny",
                "--output-dir",
                str(output_dir),
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )

    summary = json.loads((output_dir / "stream_tiny" / "benchmark_summary.json").read_text(encoding="utf-8"))
    report_text = (output_dir / "stream_tiny" / "benchmark_report.md").read_text(encoding="utf-8")

    assert summary["fixture"]["mode"] == "event_stream"
    assert summary["continuous_ingest"]["batch_count"] == 2
    assert summary["continuous_ingest"]["total_event_count"] == 6
    assert summary["continuous_ingest"]["events_per_second"] >= 0.0
    assert Path(summary["continuous_ingest"]["last_stream_run_summary_path"]).exists()
    assert "## Continuous Ingest" in report_text
