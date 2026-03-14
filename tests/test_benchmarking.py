from contextlib import contextmanager
import json
import os
import shutil
from pathlib import Path

import etl_identity_engine.cli as cli_module
from etl_identity_engine.benchmark_runtime import BenchmarkExecutionContext
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
    assert summary["deployment_profile"]["state_store_backend"] == "sqlite"
    assert summary["slo_metrics"]["latency"]["end_to_end_duration_seconds"] >= 0.0
    assert summary["slo_metrics"]["throughput"]["normalize_records_per_second"] >= 0.0
    assert summary["run_summary"]["performance"]["phase_metrics"]["normalize"]["output_record_count"] == 96
    assert "## SLO Metrics" in report_text
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
    assert summary["continuous_ingest"]["p95_batch_duration_seconds"] >= 0.0
    assert summary["slo_metrics"]["continuous_ingest"]["p95_batch_duration_seconds"] >= 0.0
    assert Path(summary["continuous_ingest"]["last_stream_run_summary_path"]).exists()
    assert "## Continuous Ingest" in report_text


def test_benchmark_run_supports_clustered_target_metadata(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    shutil.copytree(default_config_dir(), config_dir)
    (config_dir / "benchmark_fixtures.yml").write_text(
        """
benchmark_fixtures:
  - name: cluster_stream_tiny
    description: Tiny clustered continuous-ingest benchmark fixture.
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
      cluster_postgresql_baseline:
        runtime_environment: cluster
        state_store_backend: postgresql
        max_total_duration_seconds: 120.0
        min_normalize_records_per_second: 0.0
        min_match_candidate_pairs_per_second: 0.0
        max_stream_batch_duration_seconds: 120.0
        max_p95_stream_batch_duration_seconds: 120.0
        min_stream_events_per_second: 0.0
""".strip()
        + "\n",
        encoding="utf-8",
    )

    @contextmanager
    def fake_benchmark_execution_context(
        *,
        benchmark_root: Path,
        target,
        explicit_state_db: str | None,
        runtime_environment: str | None,
    ):
        state_db = str(benchmark_root / "state" / "cluster_benchmark.sqlite")
        original_values = {
            "ETL_IDENTITY_STATE_DB": os.environ.get("ETL_IDENTITY_STATE_DB"),
            "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY": os.environ.get(
                "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY"
            ),
            "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY": os.environ.get(
                "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY"
            ),
            "ETL_IDENTITY_SERVICE_READER_API_KEY": os.environ.get(
                "ETL_IDENTITY_SERVICE_READER_API_KEY"
            ),
            "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY": os.environ.get(
                "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY"
            ),
        }
        os.environ.update(
            {
                "ETL_IDENTITY_STATE_DB": state_db,
                "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY": "disabled",
                "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY": "disabled",
                "ETL_IDENTITY_SERVICE_READER_API_KEY": "cluster-reader-secret",
                "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY": "cluster-operator-secret",
            }
        )
        try:
            yield BenchmarkExecutionContext(
                state_db=state_db,
                state_store_backend="postgresql",
                runtime_environment=runtime_environment,
                state_db_display_name="postgresql+psycopg://etl_identity:***@127.0.0.1:5432/identity_state",
                state_store_mode="test_stub",
            )
        finally:
            for key, value in original_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    monkeypatch.setattr(cli_module, "benchmark_execution_context", fake_benchmark_execution_context)

    output_dir = tmp_path / "benchmarks"
    assert (
        main(
            [
                "benchmark-run",
                "--fixture",
                "cluster_stream_tiny",
                "--deployment-target",
                "cluster_postgresql_baseline",
                "--output-dir",
                str(output_dir),
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )

    summary = json.loads(
        (output_dir / "cluster_stream_tiny" / "benchmark_summary.json").read_text(encoding="utf-8")
    )
    report_text = (
        output_dir / "cluster_stream_tiny" / "benchmark_report.md"
    ).read_text(encoding="utf-8")

    assert summary["deployment_target"] == "cluster_postgresql_baseline"
    assert summary["deployment_profile"]["runtime_environment"] == "cluster"
    assert summary["deployment_profile"]["state_store_backend"] == "postgresql"
    assert summary["deployment_profile"]["state_store_mode"] == "test_stub"
    assert summary["capacity_assertions"]["status"] == "passed"
    assert summary["slo_metrics"]["continuous_ingest"]["max_batch_duration_seconds"] >= 0.0
    assert summary["slo_metrics"]["continuous_ingest"]["p95_batch_duration_seconds"] >= 0.0
    assert "State-store backend" in report_text
