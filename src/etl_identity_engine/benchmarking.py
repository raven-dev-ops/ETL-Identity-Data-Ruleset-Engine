"""Benchmark helpers for scale fixtures and capacity-target evaluation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from math import ceil

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


def _performance_total_duration(run_summary: dict[str, object]) -> float:
    performance = run_summary.get("performance", {})
    if not isinstance(performance, dict):
        raise ValueError("run summary is missing a performance block")
    total_duration_value = performance.get("total_duration_seconds")
    if isinstance(total_duration_value, bool) or not isinstance(total_duration_value, (int, float)):
        raise ValueError("run summary performance block is missing total_duration_seconds")
    return float(total_duration_value)


def _nearest_rank_percentile(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("percentile values must be non-empty")
    if percentile <= 0.0:
        return float(min(values))
    if percentile >= 100.0:
        return float(max(values))
    ordered = sorted(float(value) for value in values)
    index = max(1, ceil((percentile / 100.0) * len(ordered))) - 1
    return ordered[index]


def build_slo_metrics(
    run_summary: dict[str, object],
    *,
    continuous_ingest: dict[str, object] | None = None,
) -> dict[str, object]:
    metrics: dict[str, object] = {
        "latency": {
            "end_to_end_duration_seconds": _round_float(_performance_total_duration(run_summary)),
            "normalize_duration_seconds": _round_float(
                _performance_phase_metric(run_summary, "normalize", "duration_seconds")
            ),
            "match_duration_seconds": _round_float(
                _performance_phase_metric(run_summary, "match", "duration_seconds")
            ),
        },
        "throughput": {
            "normalize_records_per_second": _round_float(
                _performance_phase_metric(run_summary, "normalize", "output_records_per_second")
            ),
            "match_candidate_pairs_per_second": _round_float(
                _performance_phase_metric(run_summary, "match", "candidate_pairs_per_second")
            ),
        },
    }
    if not isinstance(continuous_ingest, dict) or not continuous_ingest:
        return metrics

    batch_durations = continuous_ingest.get("batch_durations_seconds", [])
    if not isinstance(batch_durations, list):
        raise ValueError("continuous_ingest.batch_durations_seconds must be a list")
    resolved_durations = [
        float(value)
        for value in batch_durations
        if not isinstance(value, bool) and isinstance(value, (int, float))
    ]
    if len(resolved_durations) != len(batch_durations):
        raise ValueError("continuous_ingest.batch_durations_seconds must contain only numbers")

    total_event_count = continuous_ingest.get("total_event_count", 0)
    events_per_second = continuous_ingest.get("events_per_second", 0.0)
    if isinstance(total_event_count, bool) or not isinstance(total_event_count, int):
        raise ValueError("continuous_ingest.total_event_count must be an integer")
    if isinstance(events_per_second, bool) or not isinstance(events_per_second, (int, float)):
        raise ValueError("continuous_ingest.events_per_second must be numeric")

    metrics["continuous_ingest"] = {
        "batch_count": len(resolved_durations),
        "total_event_count": int(total_event_count),
        "events_per_second": _round_float(float(events_per_second)),
        "max_batch_duration_seconds": _round_float(max(resolved_durations, default=0.0)),
        "p95_batch_duration_seconds": _round_float(
            _nearest_rank_percentile(resolved_durations, 95.0) if resolved_durations else 0.0
        ),
    }
    return metrics


def build_capacity_assertions(
    run_summary: dict[str, object],
    *,
    target: BenchmarkCapacityTargetConfig | None,
    continuous_ingest: dict[str, object] | None = None,
) -> dict[str, object]:
    if target is None:
        return {
            "status": "skipped",
            "deployment_name": "",
            "checks": [],
        }

    total_duration_value = _performance_total_duration(run_summary)

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

    if (
        target.max_stream_batch_duration_seconds is not None
        or target.max_p95_stream_batch_duration_seconds is not None
        or target.min_stream_events_per_second is not None
    ):
        slo_metrics = build_slo_metrics(run_summary, continuous_ingest=continuous_ingest)
        continuous_ingest_metrics = slo_metrics.get("continuous_ingest", {})
        if not isinstance(continuous_ingest_metrics, dict) or not continuous_ingest_metrics:
            raise ValueError("stream capacity targets require continuous-ingest benchmark metrics")

        if target.max_stream_batch_duration_seconds is not None:
            max_batch_duration_seconds = continuous_ingest_metrics.get("max_batch_duration_seconds")
            if isinstance(max_batch_duration_seconds, bool) or not isinstance(
                max_batch_duration_seconds, (int, float)
            ):
                raise ValueError("continuous_ingest.max_batch_duration_seconds must be numeric")
            checks.append(
                BenchmarkCapacityCheckResult(
                    name="max_stream_batch_duration_seconds",
                    operator="<=",
                    actual=_round_float(float(max_batch_duration_seconds)),
                    expected=_round_float(target.max_stream_batch_duration_seconds),
                    passed=float(max_batch_duration_seconds) <= target.max_stream_batch_duration_seconds,
                )
            )

        if target.max_p95_stream_batch_duration_seconds is not None:
            p95_batch_duration_seconds = continuous_ingest_metrics.get("p95_batch_duration_seconds")
            if isinstance(p95_batch_duration_seconds, bool) or not isinstance(
                p95_batch_duration_seconds, (int, float)
            ):
                raise ValueError("continuous_ingest.p95_batch_duration_seconds must be numeric")
            checks.append(
                BenchmarkCapacityCheckResult(
                    name="max_p95_stream_batch_duration_seconds",
                    operator="<=",
                    actual=_round_float(float(p95_batch_duration_seconds)),
                    expected=_round_float(target.max_p95_stream_batch_duration_seconds),
                    passed=float(p95_batch_duration_seconds)
                    <= target.max_p95_stream_batch_duration_seconds,
                )
            )

        if target.min_stream_events_per_second is not None:
            events_per_second = continuous_ingest_metrics.get("events_per_second")
            if isinstance(events_per_second, bool) or not isinstance(events_per_second, (int, float)):
                raise ValueError("continuous_ingest.events_per_second must be numeric")
            checks.append(
                BenchmarkCapacityCheckResult(
                    name="min_stream_events_per_second",
                    operator=">=",
                    actual=_round_float(float(events_per_second)),
                    expected=_round_float(target.min_stream_events_per_second),
                    passed=float(events_per_second) >= target.min_stream_events_per_second,
                )
            )

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
    runtime_environment: str | None,
    state_store_backend: str,
    state_db_display_name: str,
    state_store_mode: str,
    continuous_ingest: dict[str, object] | None = None,
) -> dict[str, object]:
    target = fixture.capacity_targets.get(deployment_name)
    slo_metrics = build_slo_metrics(run_summary, continuous_ingest=continuous_ingest)
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
        "deployment_profile": {
            "runtime_environment": runtime_environment or "",
            "state_store_backend": state_store_backend,
            "state_db": state_db_display_name,
            "state_store_mode": state_store_mode,
        },
        "benchmark_root": str(benchmark_root),
        "run_artifact_root": str(run_artifact_root),
        "run_summary_path": str(run_summary_path),
        "run_summary": run_summary,
        "slo_metrics": slo_metrics,
        "capacity_assertions": build_capacity_assertions(
            run_summary,
            target=target,
            continuous_ingest=continuous_ingest,
        ),
    }
    if continuous_ingest:
        summary["continuous_ingest"] = continuous_ingest
    return summary


def build_benchmark_report_markdown(summary: dict[str, object]) -> str:
    fixture = summary.get("fixture", {})
    run_summary = summary.get("run_summary", {})
    capacity_assertions = summary.get("capacity_assertions", {})
    slo_metrics = summary.get("slo_metrics", {})
    performance = run_summary.get("performance", {}) if isinstance(run_summary, dict) else {}
    phase_metrics = performance.get("phase_metrics", {}) if isinstance(performance, dict) else {}
    deployment_profile = summary.get("deployment_profile", {})

    lines = [
        "# Benchmark Report",
        "",
        f"- Fixture: `{fixture.get('name', '')}`",
        f"- Mode: `{fixture.get('mode', 'batch')}`",
        f"- Deployment target: `{summary.get('deployment_target', '')}`",
        f"- Runtime environment: `{deployment_profile.get('runtime_environment', '')}`",
        f"- State-store backend: `{deployment_profile.get('state_store_backend', '')}`",
        f"- Benchmarked at: `{summary.get('benchmarked_at_utc', '')}`",
        f"- Person count: `{fixture.get('person_count', 0)}`",
        f"- Duplicate rate: `{fixture.get('duplicate_rate', 0.0)}`",
        f"- Run artifact root: `{summary.get('run_artifact_root', '')}`",
        "",
        "## SLO Metrics",
        f"- `total_duration_seconds`: `{performance.get('total_duration_seconds', 0.0)}`",
        f"- `total_records`: `{run_summary.get('total_records', 0)}`",
        f"- `candidate_pair_count`: `{run_summary.get('candidate_pair_count', 0)}`",
        "",
    ]

    latency_metrics = slo_metrics.get("latency", {}) if isinstance(slo_metrics, dict) else {}
    throughput_metrics = slo_metrics.get("throughput", {}) if isinstance(slo_metrics, dict) else {}
    if isinstance(latency_metrics, dict):
        lines.append(
            f"- `latency.end_to_end_duration_seconds`: `{latency_metrics.get('end_to_end_duration_seconds', 0.0)}`"
        )
        lines.append(
            f"- `latency.normalize_duration_seconds`: `{latency_metrics.get('normalize_duration_seconds', 0.0)}`"
        )
        lines.append(
            f"- `latency.match_duration_seconds`: `{latency_metrics.get('match_duration_seconds', 0.0)}`"
        )
    if isinstance(throughput_metrics, dict):
        lines.append(
            f"- `throughput.normalize_records_per_second`: `{throughput_metrics.get('normalize_records_per_second', 0.0)}`"
        )
        lines.append(
            f"- `throughput.match_candidate_pairs_per_second`: `{throughput_metrics.get('match_candidate_pairs_per_second', 0.0)}`"
        )

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
            f"- `max_batch_duration_seconds`: `{continuous_ingest.get('max_batch_duration_seconds', 0.0)}`"
        )
        lines.append(
            f"- `p95_batch_duration_seconds`: `{continuous_ingest.get('p95_batch_duration_seconds', 0.0)}`"
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
