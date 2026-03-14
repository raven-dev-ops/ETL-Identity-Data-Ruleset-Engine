"""Benchmark helpers for scale fixtures and capacity-target evaluation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from etl_identity_engine.observability import utc_now
from etl_identity_engine.runtime_config import BenchmarkCapacityTargetConfig, BenchmarkFixtureConfig


@dataclass(frozen=True)
class BenchmarkCapacityCheckResult:
    name: str
    operator: str
    actual: float
    expected: float
    passed: bool


def _round_float(value: float) -> float:
    return round(float(value), 6)


def _performance_phase_metric(
    run_summary: dict[str, object],
    phase_name: str,
    metric_name: str,
) -> float:
    performance = run_summary.get("performance", {})
    if not isinstance(performance, dict):
        raise ValueError("run summary is missing a performance block")
    phase_metrics = performance.get("phase_metrics", {})
    if not isinstance(phase_metrics, dict):
        raise ValueError("run summary performance block is missing phase_metrics")
    phase_metric = phase_metrics.get(phase_name, {})
    if not isinstance(phase_metric, dict):
        raise ValueError(f"run summary performance block is missing the {phase_name!r} phase")
    value = phase_metric.get(metric_name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"run summary performance phase {phase_name!r} is missing numeric metric {metric_name!r}"
        )
    return float(value)


def build_capacity_assertions(
    run_summary: dict[str, object],
    *,
    target: BenchmarkCapacityTargetConfig | None,
) -> dict[str, object]:
    if target is None:
        return {
            "status": "skipped",
            "deployment_name": "",
            "checks": [],
        }

    total_duration = run_summary.get("performance", {})
    if not isinstance(total_duration, dict):
        raise ValueError("run summary is missing a performance block")
    total_duration_value = total_duration.get("total_duration_seconds")
    if isinstance(total_duration_value, bool) or not isinstance(total_duration_value, (int, float)):
        raise ValueError("run summary performance block is missing total_duration_seconds")

    checks = [
        BenchmarkCapacityCheckResult(
            name="max_total_duration_seconds",
            operator="<=",
            actual=_round_float(float(total_duration_value)),
            expected=_round_float(target.max_total_duration_seconds),
            passed=float(total_duration_value) <= target.max_total_duration_seconds,
        ),
        BenchmarkCapacityCheckResult(
            name="min_normalize_records_per_second",
            operator=">=",
            actual=_round_float(
                _performance_phase_metric(run_summary, "normalize", "output_records_per_second")
            ),
            expected=_round_float(target.min_normalize_records_per_second),
            passed=_performance_phase_metric(run_summary, "normalize", "output_records_per_second")
            >= target.min_normalize_records_per_second,
        ),
        BenchmarkCapacityCheckResult(
            name="min_match_candidate_pairs_per_second",
            operator=">=",
            actual=_round_float(
                _performance_phase_metric(run_summary, "match", "candidate_pairs_per_second")
            ),
            expected=_round_float(target.min_match_candidate_pairs_per_second),
            passed=_performance_phase_metric(run_summary, "match", "candidate_pairs_per_second")
            >= target.min_match_candidate_pairs_per_second,
        ),
    ]

    status = "passed" if all(check.passed for check in checks) else "failed"
    return {
        "status": status,
        "deployment_name": target.deployment_name,
        "checks": [asdict(check) for check in checks],
    }


def build_benchmark_summary(
    *,
    fixture: BenchmarkFixtureConfig,
    deployment_name: str,
    run_summary: dict[str, object],
    benchmark_root: Path,
    run_artifact_root: Path,
    run_summary_path: Path,
    continuous_ingest: dict[str, object] | None = None,
) -> dict[str, object]:
    target = fixture.capacity_targets.get(deployment_name)
    summary = {
        "benchmarked_at_utc": utc_now(),
        "fixture": {
            "name": fixture.name,
            "description": fixture.description,
            "mode": fixture.mode,
            "profile": fixture.profile,
            "person_count": fixture.person_count,
            "duplicate_rate": fixture.duplicate_rate,
            "seed": fixture.seed,
            "formats": list(fixture.formats),
            "stream_batch_count": fixture.stream_batch_count,
            "stream_events_per_batch": fixture.stream_events_per_batch,
        },
        "deployment_target": deployment_name,
        "benchmark_root": str(benchmark_root),
        "run_artifact_root": str(run_artifact_root),
        "run_summary_path": str(run_summary_path),
        "run_summary": run_summary,
        "capacity_assertions": build_capacity_assertions(run_summary, target=target),
    }
    if continuous_ingest:
        summary["continuous_ingest"] = continuous_ingest
    return summary


def build_benchmark_report_markdown(summary: dict[str, object]) -> str:
    fixture = summary.get("fixture", {})
    run_summary = summary.get("run_summary", {})
    capacity_assertions = summary.get("capacity_assertions", {})
    performance = run_summary.get("performance", {}) if isinstance(run_summary, dict) else {}
    phase_metrics = performance.get("phase_metrics", {}) if isinstance(performance, dict) else {}

    lines = [
        "# Benchmark Report",
        "",
        f"- Fixture: `{fixture.get('name', '')}`",
        f"- Mode: `{fixture.get('mode', 'batch')}`",
        f"- Deployment target: `{summary.get('deployment_target', '')}`",
        f"- Benchmarked at: `{summary.get('benchmarked_at_utc', '')}`",
        f"- Person count: `{fixture.get('person_count', 0)}`",
        f"- Duplicate rate: `{fixture.get('duplicate_rate', 0.0)}`",
        f"- Run artifact root: `{summary.get('run_artifact_root', '')}`",
        "",
        "## Throughput and Latency",
        f"- `total_duration_seconds`: `{performance.get('total_duration_seconds', 0.0)}`",
        f"- `total_records`: `{run_summary.get('total_records', 0)}`",
        f"- `candidate_pair_count`: `{run_summary.get('candidate_pair_count', 0)}`",
        "",
    ]

    for phase_name in (
        "generate",
        "normalize",
        "match",
        "cluster",
        "review_queue",
        "golden",
        "crosswalk",
        "report",
        "persist_state",
    ):
        metrics = phase_metrics.get(phase_name, {})
        if not isinstance(metrics, dict) or not metrics:
            continue
        lines.append(
            f"- `{phase_name}`: duration=`{metrics.get('duration_seconds', 0.0)}`, "
            f"input_records=`{metrics.get('input_record_count', 0)}`, "
            f"output_records=`{metrics.get('output_record_count', 0)}`, "
            f"output_records_per_second=`{metrics.get('output_records_per_second', 0.0)}`, "
            f"candidate_pairs_per_second=`{metrics.get('candidate_pairs_per_second', 0.0)}`"
        )

    continuous_ingest = summary.get("continuous_ingest", {})
    if isinstance(continuous_ingest, dict) and continuous_ingest:
        lines.extend(["", "## Continuous Ingest"])
        lines.append(f"- `batch_count`: `{continuous_ingest.get('batch_count', 0)}`")
        lines.append(f"- `total_event_count`: `{continuous_ingest.get('total_event_count', 0)}`")
        lines.append(
            f"- `total_stream_duration_seconds`: `{continuous_ingest.get('total_stream_duration_seconds', 0.0)}`"
        )
        lines.append(
            f"- `events_per_second`: `{continuous_ingest.get('events_per_second', 0.0)}`"
        )
        lines.append(
            f"- `last_stream_run_summary_path`: `{continuous_ingest.get('last_stream_run_summary_path', '')}`"
        )

    lines.extend(["", "## Capacity Assertions"])
    lines.append(f"- Status: `{capacity_assertions.get('status', 'skipped')}`")
    for check in capacity_assertions.get("checks", []):
        if not isinstance(check, dict):
            continue
        lines.append(
            f"- `{check.get('name', '')}`: actual=`{check.get('actual', 0.0)}` "
            f"{check.get('operator', '')} expected=`{check.get('expected', 0.0)}` "
            f"passed=`{check.get('passed', False)}`"
        )

    return "\n".join(lines)
