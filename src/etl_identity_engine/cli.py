"""Command-line entrypoint for the ETL Identity Engine scaffold."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import shutil
from collections import Counter
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import time
from typing import Sequence

from etl_identity_engine.benchmarking import (
    build_benchmark_report_markdown,
    build_benchmark_summary,
)
from etl_identity_engine.benchmark_runtime import benchmark_execution_context
from etl_identity_engine.delivery_publish import publish_delivery_snapshot
from etl_identity_engine.generate.synth_generator import generate_synthetic_sources
from etl_identity_engine.incremental_refresh import refresh_incremental_run
from etl_identity_engine.ingest.manifest import (
    ResolvedBatchManifest,
    peek_manifest_batch_id,
    resolve_batch_manifest,
)
from etl_identity_engine.ingest.stream_events import ResolvedStreamEventBatch, resolve_stream_event_batch
from etl_identity_engine.ingest.replay_bundle import (
    REPLAY_BUNDLE_RESTORE_MODE,
    REPLAY_BUNDLE_VERSION,
    archive_replay_bundle,
    replay_bundle_manifest_path_from_summary,
    replay_bundle_root_for_run,
    replay_bundle_summary_from_result,
    utc_now as replay_bundle_utc_now,
    verify_replay_bundle,
)
from etl_identity_engine.io.read import read_dict_rows
from etl_identity_engine.io.write import write_csv_dicts, write_markdown
from etl_identity_engine.matching.blocking import BlockingPassMetric, generate_candidates_with_metrics
from etl_identity_engine.matching.clustering import assign_cluster_ids
from etl_identity_engine.matching.scoring import classify_score, explain_pair_score
from etl_identity_engine.normalize.addresses import normalize_address
from etl_identity_engine.normalize.dates import normalize_date
from etl_identity_engine.normalize.names import normalize_name
from etl_identity_engine.normalize.phones import normalize_phone
from etl_identity_engine.observability import emit_structured_log, seconds_since
from etl_identity_engine.operator_actions import (
    apply_review_decision_operation,
    export_job_run_operation,
    publish_run_operation,
    replay_run_operation,
)
from etl_identity_engine.output_contracts import (
    BLOCKING_METRICS_HEADERS,
    CROSSWALK_HEADERS,
    DELIVERY_CONTRACT_NAME,
    DELIVERY_CONTRACT_VERSION,
    ENTITY_CLUSTER_HEADERS,
    EXCEPTION_HEADERS,
    GOLDEN_HEADERS,
    MANUAL_REVIEW_HEADERS,
    MATCH_SCORE_HEADERS,
    NORMALIZED_HEADERS,
    PUBLIC_SAFETY_GOLDEN_ACTIVITY_HEADERS,
    PUBLIC_SAFETY_INCIDENT_IDENTITY_HEADERS,
)
from etl_identity_engine.public_safety_demo import (
    build_public_safety_demo,
    build_public_safety_demo_dashboard_html,
    build_public_safety_demo_report_markdown,
    build_public_safety_demo_walkthrough_markdown,
)
from etl_identity_engine.review_cases import REVIEW_CASE_STATUSES
from etl_identity_engine.review_cases import (
    apply_review_decisions,
    build_review_case_rows,
    build_review_override_map,
    filter_active_review_queue_rows,
)
from etl_identity_engine.quality.exceptions import (
    build_run_report_markdown,
    build_run_summary,
    extract_exception_rows,
)
from etl_identity_engine.runtime_config import (
    ExportJobConfig,
    PipelineConfig,
    load_benchmark_fixture_configs,
    load_export_job_configs,
    load_pipeline_config,
    load_runtime_environment,
)
from etl_identity_engine.service_api import create_service_app
from etl_identity_engine.storage.migration_runner import (
    current_state_store_revision,
    head_revision,
    upgrade_state_store,
)
from etl_identity_engine.storage.sqlite_store import (
    EXPORT_RUN_STATUSES,
    ExportJobRunRecord,
    PipelineStateStore,
    PersistedReviewCase,
    PersistRunMetadata,
    PipelineRunRecord,
    RunCheckpointRecord,
    RUN_CHECKPOINT_STAGE_ORDER,
    build_run_key,
)
from etl_identity_engine.storage.state_store_target import state_store_display_name, state_store_reference_name
from etl_identity_engine.streaming import (
    apply_stream_event_batch,
    synthesize_stream_events,
    write_stream_events_jsonl,
)
from etl_identity_engine.survivorship.rules_engine import build_golden_records


def _load_config(config_dir: str | None, environment: str | None = None) -> PipelineConfig:
    return load_pipeline_config(
        Path(config_dir) if config_dir else None,
        environment=environment,
    )


def _resolve_runtime_environment(args: argparse.Namespace):
    environment = getattr(args, "environment", None)
    runtime_config = getattr(args, "runtime_config", None)
    if environment is None and runtime_config is None and os.environ.get("ETL_IDENTITY_ENV") is None:
        return None
    return load_runtime_environment(
        environment,
        Path(runtime_config) if runtime_config else None,
    )


def _apply_runtime_defaults(args: argparse.Namespace) -> None:
    runtime_environment = _resolve_runtime_environment(args)
    if runtime_environment is None:
        return

    if hasattr(args, "config_dir") and getattr(args, "config_dir") is None:
        args.config_dir = str(runtime_environment.config_dir)

    state_db_should_default = False
    command = getattr(args, "command", None)
    if command in {
        "state-db-upgrade",
        "state-db-current",
        "review-case-list",
        "review-case-update",
        "apply-review-decision",
        "verify-replay-bundle",
        "publish-delivery",
        "publish-run",
        "export-job-run",
        "export-job-history",
        "replay-run",
        "serve-api",
        "run-all",
        "stream-refresh",
    }:
        state_db_should_default = True
    if command == "report" and getattr(args, "run_id", None):
        state_db_should_default = True

    if (
        state_db_should_default
        and hasattr(args, "state_db")
        and getattr(args, "state_db") is None
        and runtime_environment.state_db is not None
    ):
        args.state_db = str(runtime_environment.state_db)

    args.environment = runtime_environment.name


def _require_state_db(args: argparse.Namespace) -> str:
    if not args.state_db:
        raise ValueError("This command requires --state-db or a runtime environment with state_db configured")
    return str(args.state_db)


def _config_fingerprint(config: PipelineConfig) -> str:
    payload = json.dumps(asdict(config), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()


def _normalize_rows(rows: list[dict[str, str]], config: PipelineConfig) -> list[dict[str, str]]:
    normalized_rows = []
    for row in rows:
        name_parts = [row.get("first_name", "").strip(), row.get("last_name", "").strip()]
        raw_name = " ".join(part for part in name_parts if part)
        normalized_rows.append(
            {
                **row,
                "canonical_name": normalize_name(
                    raw_name,
                    trim_whitespace=config.normalization.name.trim_whitespace,
                    remove_punctuation=config.normalization.name.remove_punctuation,
                    uppercase=config.normalization.name.uppercase,
                ),
                "canonical_dob": normalize_date(
                    row.get("dob", ""),
                    accepted_formats=config.normalization.date.accepted_formats,
                    output_format=config.normalization.date.output_format,
                )
                or "",
                "canonical_address": normalize_address(row.get("address", "")),
                "canonical_phone": normalize_phone(
                    row.get("phone", ""),
                    digits_only=config.normalization.phone.digits_only,
                    output_format=config.normalization.phone.output_format,
                    default_country_code=config.normalization.phone.default_country_code,
                ),
            }
        )
    return normalized_rows


def _read_rows(paths: Sequence[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in paths:
        rows.extend(read_dict_rows(path))
    return rows


def _discover_normalize_input_paths(input_dir: Path) -> tuple[Path, ...]:
    csv_paths = tuple(sorted(input_dir.glob("person_source_*.csv")))
    if csv_paths:
        return csv_paths

    parquet_paths = tuple(sorted(input_dir.glob("person_source_*.parquet")))
    if parquet_paths:
        return parquet_paths

    raise FileNotFoundError(
        "No normalization input files found in "
        f"{input_dir} matching person_source_*.csv or person_source_*.parquet"
    )


def _resolve_normalize_input_paths(args: argparse.Namespace) -> tuple[Path, ...]:
    explicit_inputs = tuple(Path(path) for path in (args.input or []))
    if explicit_inputs:
        return explicit_inputs
    return _discover_normalize_input_paths(Path(args.input_dir))


DEFAULT_NORMALIZE_INPUT_DIR = "data/synthetic_sources"


def _resolve_manifest_inputs(manifest_path: str) -> ResolvedBatchManifest:
    return resolve_batch_manifest(Path(manifest_path))


def _resolve_normalize_inputs(
    args: argparse.Namespace,
) -> tuple[tuple[Path, ...], list[dict[str, str]], ResolvedBatchManifest | None]:
    manifest_path = getattr(args, "manifest", None)
    if manifest_path:
        if args.input:
            raise ValueError("normalize cannot combine --manifest with --input")
        if args.input_dir != DEFAULT_NORMALIZE_INPUT_DIR:
            raise ValueError("normalize cannot combine --manifest with --input-dir")
        resolved_manifest = _resolve_manifest_inputs(manifest_path)
        return resolved_manifest.input_paths, resolved_manifest.all_rows(), resolved_manifest

    input_paths = _resolve_normalize_input_paths(args)
    return input_paths, _read_rows(input_paths), None


def _parse_formats(value: str) -> tuple[str, ...]:
    formats = tuple(part.strip().lower() for part in value.split(",") if part.strip())
    if not formats:
        raise ValueError("At least one format must be provided")
    return formats


def _resolve_generated_person_input_paths(
    input_dir: Path,
    *,
    formats: Sequence[str],
) -> tuple[Path, ...]:
    normalized_formats = tuple(fmt.strip().lower() for fmt in formats if fmt.strip())
    if "csv" in normalized_formats:
        return (
            input_dir / "person_source_a.csv",
            input_dir / "person_source_b.csv",
        )
    if "parquet" in normalized_formats:
        return (
            input_dir / "person_source_a.parquet",
            input_dir / "person_source_b.parquet",
        )
    raise ValueError(
        "run-all requires a supported input format for normalization: csv or parquet"
    )


def _resolve_optional_dataset_path(
    dataset_dir: Path,
    dataset_name: str,
    *,
    explicit_path: str | None = None,
    missing_ok: bool = False,
) -> Path | None:
    if explicit_path:
        path = Path(explicit_path)
        if path.exists():
            return path
        if missing_ok:
            return None
        raise FileNotFoundError(f"Input file not found: {path}")

    csv_path = dataset_dir / f"{dataset_name}.csv"
    if csv_path.exists():
        return csv_path

    parquet_path = dataset_dir / f"{dataset_name}.parquet"
    if parquet_path.exists():
        return parquet_path

    if missing_ok:
        return None
    raise FileNotFoundError(
        f"No input files found for {dataset_name} in {dataset_dir} matching *.csv or *.parquet"
    )


def _write_public_safety_demo_outputs(
    *,
    base_dir: Path,
    incident_records_path: str | None = None,
    incident_links_path: str | None = None,
    golden_file_path: str | None = None,
    crosswalk_file_path: str | None = None,
    output_dir_path: str | None = None,
    missing_ok: bool = False,
) -> bool:
    synthetic_dir = base_dir / "data" / "synthetic_sources"
    incident_records_file = _resolve_optional_dataset_path(
        synthetic_dir,
        "incident_records",
        explicit_path=incident_records_path,
        missing_ok=missing_ok,
    )
    incident_links_file = _resolve_optional_dataset_path(
        synthetic_dir,
        "incident_person_links",
        explicit_path=incident_links_path,
        missing_ok=missing_ok,
    )
    if incident_records_file is None or incident_links_file is None:
        return False

    golden_file = Path(golden_file_path) if golden_file_path else base_dir / "data" / "golden" / "golden_person_records.csv"
    crosswalk_file = (
        Path(crosswalk_file_path)
        if crosswalk_file_path
        else base_dir / "data" / "golden" / "source_to_golden_crosswalk.csv"
    )
    output_dir = Path(output_dir_path) if output_dir_path else base_dir / "data" / "public_safety_demo"

    result = build_public_safety_demo(
        incident_rows=read_dict_rows(incident_records_file),
        incident_link_rows=read_dict_rows(incident_links_file),
        golden_rows=read_dict_rows(golden_file),
        crosswalk_rows=read_dict_rows(crosswalk_file),
    )

    incident_identity_path = output_dir / "incident_identity_view.csv"
    golden_activity_path = output_dir / "golden_person_activity.csv"
    dashboard_path = output_dir / "public_safety_demo_dashboard.html"
    report_path = output_dir / "public_safety_demo_report.md"
    scenarios_path = output_dir / "public_safety_demo_scenarios.json"
    summary_path = output_dir / "public_safety_demo_summary.json"
    walkthrough_path = output_dir / "public_safety_demo_walkthrough.md"

    write_csv_dicts(
        incident_identity_path,
        result.incident_identity_rows,
        fieldnames=PUBLIC_SAFETY_INCIDENT_IDENTITY_HEADERS,
    )
    write_csv_dicts(
        golden_activity_path,
        result.golden_person_activity_rows,
        fieldnames=PUBLIC_SAFETY_GOLDEN_ACTIVITY_HEADERS,
    )
    write_markdown(
        dashboard_path,
        build_public_safety_demo_dashboard_html(
            summary=result.summary,
            golden_person_activity_rows=result.golden_person_activity_rows,
            incident_identity_rows=result.incident_identity_rows,
        ),
    )
    write_markdown(report_path, build_public_safety_demo_report_markdown(result.summary))
    scenarios_path.write_text(
        json.dumps(result.summary.get("demo_scenarios", []), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(result.summary, indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(walkthrough_path, build_public_safety_demo_walkthrough_markdown(result.summary))

    print(f"public safety incident view written: {incident_identity_path}")
    print(f"public safety activity view written: {golden_activity_path}")
    print(f"public safety dashboard written: {dashboard_path}")
    print(f"public safety demo report written: {report_path}")
    print(f"public safety demo scenarios written: {scenarios_path}")
    print(f"public safety demo summary written: {summary_path}")
    print(f"public safety demo walkthrough written: {walkthrough_path}")
    return True


def _default_data_root(input_file: Path) -> Path:
    if input_file.parent.name == "normalized":
        return input_file.parent.parent
    return input_file.parent


def _resolve_related_artifact(
    input_file: Path,
    explicit_path: str | None,
    *relative_path: str,
) -> Path:
    if explicit_path:
        return Path(explicit_path)
    return _default_data_root(input_file).joinpath(*relative_path)


def _resolve_match_rows(
    input_file: Path,
    explicit_matches: str | None,
) -> list[dict[str, str]]:
    matches_file = _resolve_related_artifact(
        input_file,
        explicit_matches,
        "matches",
        "candidate_scores.csv",
    )
    return read_dict_rows(matches_file)


def _build_cluster_assignment_index(cluster_rows: list[dict[str, str]]) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for row in cluster_rows:
        source_record_id = str(row.get("source_record_id", "")).strip()
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not source_record_id:
            raise ValueError("Cluster assignment rows must include source_record_id")
        if not cluster_id:
            raise ValueError(f"Cluster assignment for {source_record_id} is missing cluster_id")

        existing_cluster_id = assignments.get(source_record_id)
        if existing_cluster_id is not None and existing_cluster_id != cluster_id:
            raise ValueError(
                f"Cluster assignment for {source_record_id} is duplicated with conflicting cluster_id values"
            )
        assignments[source_record_id] = cluster_id
    return assignments


def _rows_include_cluster_ids(rows: list[dict[str, str]]) -> bool:
    record_rows = [
        row for row in rows if str(row.get("source_record_id", "")).strip()
    ]
    return bool(record_rows) and all(str(row.get("cluster_id", "")).strip() for row in record_rows)


def _apply_cluster_assignments(
    rows: list[dict[str, str]],
    cluster_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    assignments = _build_cluster_assignment_index(cluster_rows)
    enriched_rows: list[dict[str, str]] = []

    for row in rows:
        source_record_id = str(row.get("source_record_id", "")).strip()
        if not source_record_id:
            raise ValueError("Golden-record input rows must include source_record_id")

        cluster_id = assignments.get(source_record_id, "").strip()
        if not cluster_id:
            raise ValueError(
                f"No cluster assignment found for source_record_id {source_record_id}"
            )
        enriched_rows.append({**row, "cluster_id": cluster_id})

    return enriched_rows


def _resolve_golden_input_rows(args: argparse.Namespace) -> list[dict[str, str]]:
    input_file = Path(args.input)
    rows = read_dict_rows(input_file)
    if _rows_include_cluster_ids(rows):
        return rows

    cluster_file = _resolve_related_artifact(
        input_file,
        args.clusters,
        "matches",
        "entity_clusters.csv",
    )
    return _apply_cluster_assignments(rows, read_dict_rows(cluster_file))


def _resolve_cluster_command_inputs(args: argparse.Namespace) -> tuple[list[dict[str, str]], list[dict[str, str | float]]]:
    input_file = Path(args.input)
    rows = read_dict_rows(input_file)
    match_rows = _resolve_match_rows(input_file, args.matches)
    return rows, match_rows


def _collect_report_context_from_store(
    args: argparse.Namespace,
) -> tuple[str, list[dict[str, str]], dict[str, int], int, int, int, int, dict[str, object]]:
    if not args.state_db or not args.run_id:
        raise ValueError("report requires both --state-db and --run-id for persisted-state reload")
    if args.matches or args.clusters or args.golden_file or args.review_queue:
        raise ValueError("report cannot combine persisted-state reload with file override flags")

    store = PipelineStateStore(args.state_db)
    bundle = store.load_run_bundle(args.run_id)
    decision_counts = Counter(str(row.get("decision", "")) for row in bundle.candidate_pairs)
    cluster_count = len(
        {
            str(row.get("cluster_id", "")).strip()
            for row in bundle.cluster_rows
            if str(row.get("cluster_id", "")).strip()
        }
    )
    return (
        f"state-db://{state_store_reference_name(args.state_db)}?run_id={args.run_id}",
        bundle.normalized_rows,
        dict(decision_counts),
        len(bundle.candidate_pairs),
        cluster_count,
        len(bundle.golden_rows),
        len(filter_active_review_queue_rows(bundle.review_rows)),
        {
            key: bundle.run.summary[key]
            for key in ("refresh", "resume", "run_context", "performance")
            if key in bundle.run.summary
        },
    )


def _collect_report_context(
    args: argparse.Namespace,
) -> tuple[Path | str, list[dict[str, str]], dict[str, int], int, int, int, int, dict[str, object]]:
    if args.state_db or args.run_id:
        return _collect_report_context_from_store(args)

    input_file = Path(args.input)
    normalized_rows = read_dict_rows(input_file)
    match_rows = _resolve_match_rows(input_file, args.matches)
    cluster_rows = read_dict_rows(
        _resolve_related_artifact(
            input_file,
            args.clusters,
            "matches",
            "entity_clusters.csv",
        )
    )
    golden_rows = read_dict_rows(
        _resolve_related_artifact(
            input_file,
            args.golden_file,
            "golden",
            "golden_person_records.csv",
        )
    )
    review_rows = read_dict_rows(
        _resolve_related_artifact(
            input_file,
            args.review_queue,
            "review_queue",
            "manual_review_queue.csv",
        )
    )

    decision_counts = Counter(str(row.get("decision", "")) for row in match_rows)
    cluster_count = len(
        {
            str(row.get("cluster_id", "")).strip()
            for row in cluster_rows
            if str(row.get("cluster_id", "")).strip()
        }
    )
    summary_updates: dict[str, object] = {}
    summary_file = _default_data_root(input_file) / "exceptions" / "run_summary.json"
    if summary_file.exists():
        existing_summary = json.loads(summary_file.read_text(encoding="utf-8"))
        summary_updates = {
            key: existing_summary[key]
            for key in ("refresh", "resume", "run_context", "performance")
            if key in existing_summary
        }
    return (
        input_file,
        normalized_rows,
        dict(decision_counts),
        len(match_rows),
        cluster_count,
        len(golden_rows),
        len(review_rows),
        summary_updates,
    )


def _write_normalized_output(
    output_file: Path,
    rows: list[dict[str, str]],
    config: PipelineConfig,
) -> list[dict[str, str]]:
    normalized_rows = _normalize_rows(rows, config)
    write_csv_dicts(output_file, normalized_rows, fieldnames=NORMALIZED_HEADERS)
    print(f"normalized rows written: {output_file}")
    return normalized_rows


def _build_match_rows(
    rows: list[dict[str, str]],
    config: PipelineConfig,
    *,
    forced_pairs: set[tuple[str, str]] | None = None,
    review_overrides: dict[tuple[str, str], str] | None = None,
) -> tuple[list[dict[str, str | float]], list[BlockingPassMetric]]:
    blocking_passes = [blocking_pass.fields for blocking_pass in config.matching.blocking_passes]
    blocking_pass_names = [blocking_pass.name for blocking_pass in config.matching.blocking_passes]
    pairs, blocking_metrics = generate_candidates_with_metrics(
        rows,
        blocking_passes=blocking_passes,
        pass_names=blocking_pass_names,
    )
    rows_by_id = {
        str(row.get("source_record_id", "")).strip(): row
        for row in rows
        if str(row.get("source_record_id", "")).strip()
    }
    seen_pairs = {
        tuple(sorted((str(left.get("source_record_id", "")), str(right.get("source_record_id", "")))))
        for left, right in pairs
    }
    for forced_pair in sorted(forced_pairs or set()):
        left_id, right_id = forced_pair
        if forced_pair in seen_pairs:
            continue
        left_row = rows_by_id.get(left_id)
        right_row = rows_by_id.get(right_id)
        if left_row is None or right_row is None:
            continue
        pairs.append((left_row, right_row))
        seen_pairs.add(forced_pair)

    scored_rows: list[dict[str, str | float]] = []
    for left, right in pairs:
        detail = explain_pair_score(left, right, weights=config.matching.weights)
        scored_rows.append(
            {
                "left_id": left.get("source_record_id", ""),
                "right_id": right.get("source_record_id", ""),
                "score": detail.score,
                "decision": classify_score(
                    detail.score,
                    auto_merge=config.matching.thresholds.auto_merge,
                    manual_review_min=config.matching.thresholds.manual_review_min,
                    no_match_max=config.matching.thresholds.no_match_max,
                ),
                "matched_fields": ";".join(detail.matched_fields),
                "reason_trace": ";".join(detail.reason_trace),
            }
        )
    if review_overrides:
        scored_rows = apply_review_decisions(scored_rows, review_overrides)
    scored_rows.sort(key=lambda row: (str(row.get("left_id", "")), str(row.get("right_id", ""))))
    return scored_rows, blocking_metrics


def _build_blocking_metrics_rows(
    blocking_metrics: list[BlockingPassMetric],
    *,
    overall_candidate_pair_count: int,
) -> list[dict[str, str | int]]:
    return [
        {
            "pass_name": metric.pass_name,
            "fields": ";".join(metric.fields),
            "raw_candidate_pair_count": metric.raw_candidate_pair_count,
            "new_candidate_pair_count": metric.new_candidate_pair_count,
            "cumulative_candidate_pair_count": metric.cumulative_candidate_pair_count,
            "overall_deduplicated_candidate_pair_count": overall_candidate_pair_count,
        }
        for metric in blocking_metrics
    ]


def _write_match_output(
    output_file: Path,
    rows: list[dict[str, str]],
    config: PipelineConfig,
    *,
    forced_pairs: set[tuple[str, str]] | None = None,
    review_overrides: dict[tuple[str, str], str] | None = None,
) -> tuple[list[dict[str, str | float]], list[dict[str, str | int]]]:
    scored_rows, blocking_metrics = _build_match_rows(
        rows,
        config,
        forced_pairs=forced_pairs,
        review_overrides=review_overrides,
    )
    blocking_metrics_rows = _build_blocking_metrics_rows(
        blocking_metrics,
        overall_candidate_pair_count=len(scored_rows),
    )
    write_csv_dicts(output_file, scored_rows, fieldnames=MATCH_SCORE_HEADERS)
    blocking_metrics_file = output_file.with_name("blocking_metrics.csv")
    write_csv_dicts(
        blocking_metrics_file,
        blocking_metrics_rows,
        fieldnames=BLOCKING_METRICS_HEADERS,
    )
    print(f"candidate scores written: {output_file}")
    print(f"blocking metrics written: {blocking_metrics_file}")
    return scored_rows, blocking_metrics_rows


def _build_clustered_rows(
    rows: list[dict[str, str]],
    match_rows: list[dict[str, str | float]],
) -> list[dict[str, str]]:
    record_ids = sorted(
        {
            str(row.get("source_record_id", "")).strip()
            for row in rows
            if str(row.get("source_record_id", "")).strip()
        }
    )
    accepted_links = [
        (str(row.get("left_id", "")), str(row.get("right_id", "")))
        for row in match_rows
        if row.get("decision") == "auto_merge"
    ]
    cluster_ids = assign_cluster_ids(record_ids, accepted_links)

    clustered_rows: list[dict[str, str]] = []
    for row in rows:
        source_record_id = str(row.get("source_record_id", "")).strip()
        clustered_rows.append(
            {
                **row,
                "cluster_id": cluster_ids.get(source_record_id, ""),
            }
        )
    return clustered_rows


def _write_cluster_output(
    output_file: Path,
    rows: list[dict[str, str]],
    match_rows: list[dict[str, str | float]],
) -> list[dict[str, str]]:
    clustered_rows = _build_clustered_rows(rows, match_rows)
    cluster_output_rows = [
        {
            "cluster_id": row.get("cluster_id", ""),
            "source_record_id": row.get("source_record_id", ""),
            "source_system": row.get("source_system", ""),
            "person_entity_id": row.get("person_entity_id", ""),
        }
        for row in sorted(clustered_rows, key=lambda row: row.get("source_record_id", ""))
    ]
    write_csv_dicts(output_file, cluster_output_rows, fieldnames=ENTITY_CLUSTER_HEADERS)
    print(f"cluster assignments written: {output_file}")
    return clustered_rows


def _write_manual_review_output(
    output_file: Path,
    match_rows: list[dict[str, str | float]],
    *,
    previous_review_rows: list[dict[str, str | float]] | list[dict[str, str]] | None = None,
) -> tuple[list[dict[str, str | float]], list[dict[str, str | float]]]:
    active_review_rows, persisted_review_rows = build_review_case_rows(
        match_rows,
        previous_review_cases=previous_review_rows or (),
    )
    write_csv_dicts(output_file, active_review_rows, fieldnames=MANUAL_REVIEW_HEADERS)
    print(f"manual review queue written: {output_file}")
    return active_review_rows, persisted_review_rows


def _write_golden_output(
    output_file: Path,
    rows: list[dict[str, str]],
    config: PipelineConfig,
) -> list[dict[str, str]]:
    golden_records = build_golden_records(
        rows,
        source_priority=config.survivorship.source_priority,
        field_rules=config.survivorship.field_rules,
    )
    write_csv_dicts(output_file, golden_records, fieldnames=GOLDEN_HEADERS)
    print(f"golden output written: {output_file}")
    return golden_records


def _build_crosswalk_rows(
    rows: list[dict[str, str]],
    golden_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    cluster_to_golden = {
        row.get("cluster_id", ""): row.get("golden_id", "")
        for row in golden_rows
        if row.get("cluster_id")
    }
    return [
        {
            "source_record_id": row.get("source_record_id", ""),
            "source_system": row.get("source_system", ""),
            "person_entity_id": row.get("person_entity_id", ""),
            "cluster_id": row.get("cluster_id", ""),
            "golden_id": cluster_to_golden.get(row.get("cluster_id", ""), ""),
        }
        for row in sorted(rows, key=lambda item: item.get("source_record_id", ""))
    ]


def _write_crosswalk_output(
    output_file: Path,
    rows: list[dict[str, str]],
    golden_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    crosswalk_rows = _build_crosswalk_rows(rows, golden_rows)
    write_csv_dicts(output_file, crosswalk_rows, fieldnames=CROSSWALK_HEADERS)
    print(f"crosswalk written: {output_file}")
    return crosswalk_rows


def _write_precomputed_match_outputs(
    output_file: Path,
    match_rows: list[dict[str, str | float]],
    blocking_metrics_rows: list[dict[str, str | int]],
) -> None:
    blocking_metrics_file = output_file.with_name("blocking_metrics.csv")
    write_csv_dicts(output_file, match_rows, fieldnames=MATCH_SCORE_HEADERS)
    write_csv_dicts(
        blocking_metrics_file,
        blocking_metrics_rows,
        fieldnames=BLOCKING_METRICS_HEADERS,
    )
    print(f"candidate scores written: {output_file}")
    print(f"blocking metrics written: {blocking_metrics_file}")


def _write_precomputed_cluster_output(
    output_file: Path,
    cluster_output_rows: list[dict[str, str]],
) -> None:
    write_csv_dicts(output_file, cluster_output_rows, fieldnames=ENTITY_CLUSTER_HEADERS)
    print(f"cluster assignments written: {output_file}")


def _write_precomputed_golden_output(
    output_file: Path,
    golden_rows: list[dict[str, str]],
) -> None:
    write_csv_dicts(output_file, golden_rows, fieldnames=GOLDEN_HEADERS)
    print(f"golden output written: {output_file}")


def _write_precomputed_crosswalk_output(
    output_file: Path,
    crosswalk_rows: list[dict[str, str]],
) -> None:
    write_csv_dicts(output_file, crosswalk_rows, fieldnames=CROSSWALK_HEADERS)
    print(f"crosswalk written: {output_file}")


def _write_precomputed_review_queue_output(
    output_file: Path,
    review_rows: list[dict[str, str | float]],
) -> None:
    write_csv_dicts(
        output_file,
        filter_active_review_queue_rows(review_rows),
        fieldnames=MANUAL_REVIEW_HEADERS,
    )
    print(f"manual review queue written: {output_file}")


def _write_quality_outputs(
    output_file: Path,
    input_file: Path | str,
    rows: list[dict[str, str]],
    *,
    candidate_pair_count: int = 0,
    decision_counts: dict[str, int] | None = None,
    cluster_count: int = 0,
    golden_record_count: int = 0,
    review_queue_count: int = 0,
    summary_updates: dict[str, object] | None = None,
) -> dict[str, object]:
    exception_rows = extract_exception_rows(rows)
    for exception_type, records in exception_rows.items():
        write_csv_dicts(
            output_file.parent / f"{exception_type}.csv",
            records,
            fieldnames=EXCEPTION_HEADERS,
        )

    summary = build_run_summary(
        rows,
        exception_rows=exception_rows,
        candidate_pair_count=candidate_pair_count,
        decision_counts=decision_counts,
        cluster_count=cluster_count,
        golden_record_count=golden_record_count,
        review_queue_count=review_queue_count,
    )
    if summary_updates:
        summary.update(summary_updates)

    if isinstance(input_file, Path):
        try:
            display_input_path = os.path.relpath(input_file, output_file.parent).replace("\\", "/")
        except ValueError:
            display_input_path = str(input_file).replace("\\", "/")
    else:
        display_input_path = input_file

    write_markdown(output_file, build_run_report_markdown(display_input_path, summary))

    summary_path = output_file.with_name("run_summary.json")
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    print(f"report written: {output_file}")
    print(f"run summary written: {summary_path}")
    return summary


def _rate(count: int | float, duration_seconds: float) -> float:
    if duration_seconds <= 0.0:
        return 0.0
    return round(float(count) / duration_seconds, 6)


def _build_phase_metric(
    duration_seconds: float,
    *,
    input_record_count: int = 0,
    output_record_count: int = 0,
    candidate_pair_count: int = 0,
) -> dict[str, float | int]:
    return {
        "duration_seconds": round(duration_seconds, 6),
        "input_record_count": int(input_record_count),
        "output_record_count": int(output_record_count),
        "output_records_per_second": _rate(output_record_count, duration_seconds),
        "candidate_pair_count": int(candidate_pair_count),
        "candidate_pairs_per_second": _rate(candidate_pair_count, duration_seconds),
    }


def _deep_copy_phase_metrics(
    phase_metrics: dict[str, dict[str, float | int]],
) -> dict[str, dict[str, float | int]]:
    return {
        phase_name: dict(metrics)
        for phase_name, metrics in phase_metrics.items()
    }


def _build_cluster_output_rows(clustered_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "cluster_id": row.get("cluster_id", ""),
            "source_record_id": row.get("source_record_id", ""),
            "source_system": row.get("source_system", ""),
            "person_entity_id": row.get("person_entity_id", ""),
        }
        for row in sorted(clustered_rows, key=lambda row: row.get("source_record_id", ""))
    ]


def _restore_checkpoint_outputs(
    *,
    normalized_file: Path,
    matches_file: Path,
    clusters_file: Path,
    golden_file: Path,
    crosswalk_file: Path,
    review_queue_file: Path,
    checkpoint: RunCheckpointRecord,
) -> None:
    if checkpoint.normalized_rows:
        write_csv_dicts(normalized_file, checkpoint.normalized_rows, fieldnames=NORMALIZED_HEADERS)
        print(f"normalized output restored from checkpoint: {normalized_file}")
    if checkpoint.match_rows or checkpoint.blocking_metrics_rows:
        _write_precomputed_match_outputs(
            matches_file,
            checkpoint.match_rows,
            checkpoint.blocking_metrics_rows,
        )
    if checkpoint.cluster_rows:
        _write_precomputed_cluster_output(
            clusters_file,
            _build_cluster_output_rows(checkpoint.cluster_rows),
        )
    if checkpoint.review_rows:
        _write_precomputed_review_queue_output(review_queue_file, checkpoint.review_rows)
    if checkpoint.golden_rows:
        _write_precomputed_golden_output(golden_file, checkpoint.golden_rows)
    if checkpoint.crosswalk_rows:
        _write_precomputed_crosswalk_output(crosswalk_file, checkpoint.crosswalk_rows)


def _cluster_count(clustered_rows: list[dict[str, str]]) -> int:
    return len({row.get("cluster_id", "") for row in clustered_rows if row.get("cluster_id")})


def _decision_counts(match_rows: list[dict[str, str | float]]) -> dict[str, int]:
    counts = Counter(str(row.get("decision", "")) for row in match_rows)
    return {
        "auto_merge": counts.get("auto_merge", 0),
        "manual_review": counts.get("manual_review", 0),
        "no_match": counts.get("no_match", 0),
    }


def _build_resume_summary(
    *,
    resumed_from_run_id: str | None,
    resumed_from_stage: str | None,
    available_checkpoint_run_id: str | None,
    available_checkpoint_stage: str | None,
) -> dict[str, object]:
    return {
        "resumed": bool(resumed_from_run_id and resumed_from_stage),
        "resumed_from_run_id": resumed_from_run_id or "",
        "resumed_from_stage": resumed_from_stage or "",
        "resume_supported": bool(available_checkpoint_stage),
        "available_checkpoint_run_id": available_checkpoint_run_id or "",
        "available_checkpoint_stage": available_checkpoint_stage or "",
    }


def _build_pipeline_summary(
    *,
    normalized_rows: list[dict[str, str]],
    match_rows: list[dict[str, str | float]],
    clustered_rows: list[dict[str, str]],
    golden_rows: list[dict[str, str]],
    active_review_rows: list[dict[str, str | float]],
    run_context: dict[str, object],
    refresh_summary: dict[str, object],
    resume_summary: dict[str, object],
    phase_metrics: dict[str, dict[str, float | int]],
    total_duration_seconds: float,
    stream_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    summary = build_run_summary(
        normalized_rows,
        candidate_pair_count=len(match_rows),
        decision_counts=_decision_counts(match_rows),
        cluster_count=_cluster_count(clustered_rows),
        golden_record_count=len(golden_rows),
        review_queue_count=len(active_review_rows),
    )
    summary["run_context"] = run_context
    summary["refresh"] = refresh_summary
    summary["resume"] = resume_summary
    summary["performance"] = {
        "total_duration_seconds": round(total_duration_seconds, 6),
        "phase_metrics": _deep_copy_phase_metrics(phase_metrics),
    }
    if stream_summary is not None:
        summary["stream"] = stream_summary
    return summary


def _resolve_run_key_args(
    args: argparse.Namespace,
    *,
    batch_id: str | None,
) -> dict[str, object | None]:
    manifest_path = getattr(args, "manifest", None)
    return {
        "input_mode": "manifest" if manifest_path else "synthetic",
        "manifest_path": str(Path(manifest_path).resolve()) if manifest_path else None,
        "batch_id": batch_id,
        "config_dir": str(Path(args.config_dir).resolve()) if args.config_dir else None,
        "profile": None if manifest_path else args.profile,
        "seed": None if manifest_path else args.seed,
        "person_count": None if manifest_path else getattr(args, "person_count", None),
        "duplicate_rate": None if manifest_path else args.duplicate_rate,
        "formats": None if manifest_path else args.formats,
        "refresh_mode": getattr(args, "refresh_mode", "full"),
    }


def _resolve_stream_run_key_args(
    args: argparse.Namespace,
    *,
    source_run_id: str,
    batch: ResolvedStreamEventBatch,
) -> dict[str, object | None]:
    return {
        "input_mode": "event_stream",
        "manifest_path": None,
        "batch_id": batch.batch_id,
        "config_dir": str(Path(args.config_dir).resolve()) if args.config_dir else None,
        "profile": None,
        "seed": None,
        "person_count": None,
        "duplicate_rate": None,
        "formats": "jsonl",
        "refresh_mode": "incremental",
        "source_run_id": source_run_id,
        "stream_id": batch.stream_id,
        "stream_first_sequence": batch.first_sequence,
        "stream_last_sequence": batch.last_sequence,
        "stream_event_hash": batch.content_sha256,
    }


def _persist_stream_event_snapshot(
    *,
    batch: ResolvedStreamEventBatch,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if batch.event_path.resolve() == output_path.resolve():
        return output_path
    shutil.copyfile(batch.event_path, output_path)
    return output_path


def _build_stream_phase_metric(
    duration_seconds: float,
    *,
    event_count: int,
    output_record_count: int,
) -> dict[str, float | int]:
    metric = _build_phase_metric(
        duration_seconds,
        input_record_count=event_count,
        output_record_count=output_record_count,
    )
    metric["event_count"] = int(event_count)
    metric["events_per_second"] = _rate(event_count, duration_seconds)
    return metric


def _restore_persisted_run_outputs(base: Path, bundle) -> None:
    normalized_file = base / "data" / "normalized" / "normalized_person_records.csv"
    matches_file = base / "data" / "matches" / "candidate_scores.csv"
    clusters_file = base / "data" / "matches" / "entity_clusters.csv"
    golden_file = base / "data" / "golden" / "golden_person_records.csv"
    crosswalk_file = base / "data" / "golden" / "source_to_golden_crosswalk.csv"
    review_queue_file = base / "data" / "review_queue" / "manual_review_queue.csv"
    report_file = base / "data" / "exceptions" / "run_report.md"

    write_csv_dicts(normalized_file, bundle.normalized_rows, fieldnames=NORMALIZED_HEADERS)
    write_csv_dicts(matches_file, bundle.candidate_pairs, fieldnames=MATCH_SCORE_HEADERS)
    write_csv_dicts(
        matches_file.with_name("blocking_metrics.csv"),
        bundle.blocking_metrics_rows,
        fieldnames=BLOCKING_METRICS_HEADERS,
    )
    write_csv_dicts(clusters_file, bundle.cluster_rows, fieldnames=ENTITY_CLUSTER_HEADERS)
    write_csv_dicts(golden_file, bundle.golden_rows, fieldnames=GOLDEN_HEADERS)
    write_csv_dicts(crosswalk_file, bundle.crosswalk_rows, fieldnames=CROSSWALK_HEADERS)
    write_csv_dicts(
        review_queue_file,
        filter_active_review_queue_rows(bundle.review_rows),
        fieldnames=MANUAL_REVIEW_HEADERS,
    )

    exception_rows = extract_exception_rows(bundle.normalized_rows)
    for exception_type, records in exception_rows.items():
        write_csv_dicts(
            report_file.parent / f"{exception_type}.csv",
            records,
            fieldnames=EXCEPTION_HEADERS,
        )

    display_input_path = os.path.relpath(normalized_file, report_file.parent).replace("\\", "/")
    write_markdown(report_file, build_run_report_markdown(display_input_path, bundle.run.summary))
    report_file.with_name("run_summary.json").write_text(
        json.dumps(bundle.run.summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_public_safety_demo_outputs(base_dir=base, missing_ok=True)


def _cmd_generate(args: argparse.Namespace) -> None:
    output_dir = Path(args.output)
    formats = _parse_formats(args.formats)
    result = generate_synthetic_sources(
        output_dir=output_dir,
        profile=args.profile,
        seed=args.seed,
        duplicate_rate=args.duplicate_rate,
        formats=formats,
        person_count_override=args.person_count,
    )
    print("generated:")
    for file_path in result.generated_files:
        print(f" - {file_path}")
    print("generation summary:")
    print(f" - profile: {result.summary['profile']}")
    print(f" - person_entity_count: {result.summary['person_entity_count']}")
    print(f" - duplicate_variant_count: {result.summary['duplicate_variant_count']}")
    print(f" - incident_count: {result.summary['incident_count']}")


def _cmd_normalize(args: argparse.Namespace) -> None:
    output_file = Path(args.output)
    input_paths, rows, resolved_manifest = _resolve_normalize_inputs(args)
    if resolved_manifest is not None:
        print(
            f"validated batch manifest: {resolved_manifest.manifest_path} "
            f"(batch_id={resolved_manifest.manifest.batch_id})"
        )
    print(f"normalizing {len(input_paths)} input file(s)")
    for path in input_paths:
        print(f" - {path}")
    _write_normalized_output(output_file, rows, _load_config(args.config_dir, args.environment))


def _cmd_match(args: argparse.Namespace) -> None:
    input_file = Path(args.input)
    output_file = Path(args.output)
    _write_match_output(
        output_file,
        read_dict_rows(input_file),
        _load_config(args.config_dir, args.environment),
    )


def _cmd_cluster(args: argparse.Namespace) -> None:
    output_file = Path(args.output)
    rows, match_rows = _resolve_cluster_command_inputs(args)
    _write_cluster_output(output_file, rows, match_rows)


def _cmd_review_queue(args: argparse.Namespace) -> None:
    input_file = Path(args.input)
    output_file = Path(args.output)
    _write_manual_review_output(output_file, read_dict_rows(input_file))


def _cmd_golden(args: argparse.Namespace) -> None:
    output_file = Path(args.output)
    _write_golden_output(
        output_file,
        _resolve_golden_input_rows(args),
        _load_config(args.config_dir, args.environment),
    )


def _cmd_public_safety_demo(args: argparse.Namespace) -> None:
    wrote_outputs = _write_public_safety_demo_outputs(
        base_dir=Path(args.base_dir),
        incident_records_path=args.incident_records,
        incident_links_path=args.incident_links,
        golden_file_path=args.golden_file,
        crosswalk_file_path=args.crosswalk_file,
        output_dir_path=args.output_dir,
        missing_ok=False,
    )
    if not wrote_outputs:
        raise FileNotFoundError("No public-safety demo inputs were found")


def _cmd_state_db_upgrade(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    upgrade_state_store(state_db, revision=args.revision)
    current_revision = current_state_store_revision(state_db) or "uninitialized"
    print(f"state db upgraded: {state_store_display_name(state_db)} (revision={current_revision})")


def _cmd_state_db_current(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    current_revision = current_state_store_revision(state_db) or "uninitialized"
    print(f"state db revision: {current_revision} (head={head_revision()})")


def _resolve_review_case_run_id(args: argparse.Namespace, store: PipelineStateStore) -> str:
    run_id = args.run_id or store.latest_completed_run_id_with_review_cases()
    if run_id is None:
        raise FileNotFoundError(
            f"No completed persisted review-case runs found in {state_store_display_name(_require_state_db(args))}"
        )
    return run_id


def _serialize_review_case(case: PersistedReviewCase) -> dict[str, object]:
    return {
        "run_id": case.run_id,
        "review_id": case.review_id,
        "left_id": case.left_id,
        "right_id": case.right_id,
        "score": case.score,
        "reason_codes": case.reason_codes,
        "top_contributing_match_signals": case.top_contributing_match_signals,
        "queue_status": case.queue_status,
        "assigned_to": case.assigned_to,
        "operator_notes": case.operator_notes,
        "created_at_utc": case.created_at_utc,
        "updated_at_utc": case.updated_at_utc,
        "resolved_at_utc": case.resolved_at_utc,
    }


def _serialize_run_record(record: PipelineRunRecord) -> dict[str, object]:
    return {
        "run_id": record.run_id,
        "run_key": record.run_key,
        "attempt_number": record.attempt_number,
        "batch_id": record.batch_id,
        "input_mode": record.input_mode,
        "manifest_path": record.manifest_path,
        "base_dir": record.base_dir,
        "config_dir": record.config_dir,
        "profile": record.profile,
        "seed": record.seed,
        "formats": record.formats,
        "status": record.status,
        "started_at_utc": record.started_at_utc,
        "finished_at_utc": record.finished_at_utc,
        "total_records": record.total_records,
        "candidate_pair_count": record.candidate_pair_count,
        "cluster_count": record.cluster_count,
        "golden_record_count": record.golden_record_count,
        "review_queue_count": record.review_queue_count,
        "failure_detail": record.failure_detail,
        "resumed_from_run_id": record.resumed_from_run_id,
        "summary": record.summary,
    }


def _write_run_summary_artifacts(
    *,
    report_file: Path,
    normalized_file: Path,
    summary: dict[str, object],
) -> None:
    report_file.write_text(
        build_run_report_markdown(
            os.path.relpath(normalized_file, report_file.parent).replace("\\", "/"),
            summary,
        ),
        encoding="utf-8",
    )
    report_file.with_name("run_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _serialize_export_job(job: ExportJobConfig) -> dict[str, object]:
    return {
        "name": job.name,
        "consumer": job.consumer,
        "description": job.description,
        "output_root": str(job.output_root),
        "contract_name": job.contract_name,
        "contract_version": job.contract_version,
        "format": job.export_format,
    }


def _serialize_export_run_record(record: ExportJobRunRecord) -> dict[str, object]:
    return {
        "export_run_id": record.export_run_id,
        "export_key": record.export_key,
        "attempt_number": record.attempt_number,
        "job_name": record.job_name,
        "source_run_id": record.source_run_id,
        "contract_name": record.contract_name,
        "contract_version": record.contract_version,
        "output_root": record.output_root,
        "status": record.status,
        "started_at_utc": record.started_at_utc,
        "finished_at_utc": record.finished_at_utc,
        "snapshot_dir": record.snapshot_dir,
        "current_pointer_path": record.current_pointer_path,
        "row_counts": record.row_counts,
        "metadata": record.metadata,
        "failure_detail": record.failure_detail,
    }


def _record_cli_audit_event(
    store: PipelineStateStore,
    *,
    action: str,
    resource_type: str,
    resource_id: str,
    status: str,
    run_id: str | None = None,
    details: dict[str, object] | None = None,
) -> None:
    store.record_audit_event(
        actor_type="cli",
        actor_id="operator",
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        run_id=run_id,
        status=status,
        details=details or {},
    )


def _resolve_completed_run_id(args: argparse.Namespace, store: PipelineStateStore) -> str:
    run_id = args.run_id or store.latest_completed_run_id()
    if run_id is None:
        raise FileNotFoundError(
            f"No completed persisted runs found in {state_store_display_name(_require_state_db(args))}"
        )
    return run_id


def _load_export_jobs(args: argparse.Namespace) -> dict[str, ExportJobConfig]:
    return load_export_job_configs(
        Path(args.config_dir) if getattr(args, "config_dir", None) else None,
        environment=getattr(args, "environment", None),
    )


def _resolve_export_job(args: argparse.Namespace) -> ExportJobConfig:
    jobs = _load_export_jobs(args)
    job = jobs.get(args.job_name)
    if job is None:
        raise FileNotFoundError(
            f"Configured export job not found: {args.job_name}. Available jobs: {sorted(jobs)}"
        )
    return job


def _cmd_review_case_list(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    run_id = _resolve_review_case_run_id(args, store)
    cases = store.list_review_cases(
        run_id=run_id,
        queue_status=args.status,
        assigned_to=args.assigned_to,
    )
    print(json.dumps([_serialize_review_case(case) for case in cases], indent=2, sort_keys=True))


def _cmd_review_case_update(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    run_id = _resolve_review_case_run_id(args, store)
    updated = store.update_review_case(
        run_id=run_id,
        review_id=args.review_id,
        queue_status=args.status,
        assigned_to=args.assigned_to,
        operator_notes=args.notes,
    )
    print(json.dumps(_serialize_review_case(updated), indent=2, sort_keys=True))


def _cmd_apply_review_decision(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    try:
        result = apply_review_decision_operation(
            store=store,
            run_id=args.run_id,
            review_id=args.review_id,
            decision=args.decision,
            assigned_to=args.assigned_to,
            notes=args.notes,
        )
    except Exception as exc:
        _record_cli_audit_event(
            store,
            action="apply_review_decision",
            resource_type="review_case",
            resource_id=args.review_id,
            run_id=args.run_id,
            status="failed",
            details={
                "decision": args.decision,
                "assigned_to": args.assigned_to or "",
                "notes": args.notes or "",
                "error": str(exc),
            },
        )
        emit_structured_log(
            "review_decision_failed",
            component="cli",
            command="apply-review-decision",
            review_id=args.review_id,
            run_id=args.run_id or "",
            duration_seconds=seconds_since(started),
            error=str(exc),
            level="ERROR",
        )
        raise

    _record_cli_audit_event(
        store,
        action="apply_review_decision",
        resource_type="review_case",
        resource_id=result.case.review_id,
        run_id=result.case.run_id,
        status="noop" if result.action == "noop" else "succeeded",
        details={
            "decision": result.case.queue_status,
            "assigned_to": result.case.assigned_to,
            "operator_notes": result.case.operator_notes,
            "action": result.action,
        },
    )
    emit_structured_log(
        "review_decision_applied",
        component="cli",
        command="apply-review-decision",
        run_id=result.case.run_id,
        review_id=result.case.review_id,
        action=result.action,
        queue_status=result.case.queue_status,
        duration_seconds=seconds_since(started),
    )

    print(
        json.dumps(
            {
                "action": result.action,
                "state_db": state_store_display_name(state_db),
                "case": _serialize_review_case(result.case),
            },
            indent=2,
            sort_keys=True,
        )
    )


def _verify_run_replay_bundle(
    *,
    store: PipelineStateStore,
    run_id: str,
) -> dict[str, object]:
    record = store.load_run_record(run_id)
    bundle_manifest_path = replay_bundle_manifest_path_from_summary(record.summary)
    if bundle_manifest_path is None:
        raise FileNotFoundError(f"Persisted run {run_id} does not have archived replay-bundle metadata")

    verification = verify_replay_bundle(bundle_manifest_path)
    updated_summary = dict(record.summary)
    updated_summary["replay_bundle"] = replay_bundle_summary_from_result(verification)
    store.update_run_summary(run_id=run_id, summary=updated_summary)
    base_dir = Path(record.base_dir)
    normalized_file = base_dir / "data" / "normalized" / "normalized_person_records.csv"
    report_file = base_dir / "data" / "exceptions" / "run_report.md"
    if normalized_file.exists() and report_file.parent.exists():
        _write_run_summary_artifacts(
            report_file=report_file,
            normalized_file=normalized_file,
            summary=updated_summary,
        )
    return {
        "run_id": run_id,
        "status": verification.status,
        "recoverable": verification.recoverable,
        "replay_bundle": replay_bundle_summary_from_result(verification),
    }


def _cmd_verify_replay_bundle(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    run_id = _resolve_completed_run_id(args, store)

    try:
        payload = _verify_run_replay_bundle(store=store, run_id=run_id)
    except Exception as exc:
        _record_cli_audit_event(
            store,
            action="verify_replay_bundle",
            resource_type="pipeline_run",
            resource_id=run_id,
            run_id=run_id,
            status="failed",
            details={"error": str(exc)},
        )
        emit_structured_log(
            "replay_bundle_verification_failed",
            component="cli",
            command="verify-replay-bundle",
            run_id=run_id,
            duration_seconds=seconds_since(started),
            error=str(exc),
            level="ERROR",
        )
        raise

    verification_status = str(payload["status"])
    _record_cli_audit_event(
        store,
        action="verify_replay_bundle",
        resource_type="pipeline_run",
        resource_id=run_id,
        run_id=run_id,
        status="succeeded" if bool(payload["recoverable"]) else "failed",
        details={"status": verification_status, "recoverable": bool(payload["recoverable"])},
    )
    emit_structured_log(
        "replay_bundle_verified",
        component="cli",
        command="verify-replay-bundle",
        run_id=run_id,
        status=verification_status,
        recoverable=bool(payload["recoverable"]),
        duration_seconds=seconds_since(started),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


def _cmd_publish_delivery(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    run_id = args.run_id or store.latest_completed_run_id()
    if run_id is None:
        raise FileNotFoundError(f"No completed persisted runs found in {state_store_display_name(state_db)}")

    bundle = store.load_run_bundle(run_id)
    contract_root = Path(args.output_dir) / DELIVERY_CONTRACT_NAME / args.contract_version
    snapshot_dir = contract_root / "snapshots" / run_id
    snapshot_existed = snapshot_dir.exists()
    published = publish_delivery_snapshot(
        bundle=bundle,
        state_db_path=state_db,
        output_root=Path(args.output_dir),
        contract_version=args.contract_version,
    )
    _record_cli_audit_event(
        store,
        action="publish_delivery",
        resource_type="pipeline_run",
        resource_id=run_id,
        run_id=run_id,
        status="reused" if snapshot_existed else "succeeded",
        details={
            "contract_version": args.contract_version,
            "snapshot_dir": str(published.snapshot_dir),
            "current_pointer_path": str(published.current_pointer_path),
        },
    )
    emit_structured_log(
        "delivery_snapshot_published",
        component="cli",
        command="publish-delivery",
        run_id=run_id,
        action="reused_snapshot" if snapshot_existed else "published",
        contract_version=args.contract_version,
        snapshot_dir=published.snapshot_dir,
        duration_seconds=seconds_since(started),
    )
    print(f"delivery snapshot published: {published.snapshot_dir}")
    print(f"delivery pointer updated: {published.current_pointer_path}")


def _cmd_publish_run(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    result = publish_run_operation(
        store=store,
        state_db=state_db,
        run_id=args.run_id,
        output_dir=Path(args.output_dir),
        contract_version=args.contract_version,
    )
    _record_cli_audit_event(
        store,
        action="publish_run",
        resource_type="pipeline_run",
        resource_id=result.run.run_id,
        run_id=result.run.run_id,
        status="reused" if result.action == "reused_snapshot" else "succeeded",
        details={
            "contract_version": result.contract_version,
            "snapshot_dir": str(result.snapshot_dir),
            "current_pointer_path": str(result.current_pointer_path),
            "action": result.action,
        },
    )
    emit_structured_log(
        "delivery_snapshot_published",
        component="cli",
        command="publish-run",
        run_id=result.run.run_id,
        action=result.action,
        contract_version=result.contract_version,
        snapshot_dir=result.snapshot_dir,
        duration_seconds=seconds_since(started),
    )
    print(
        json.dumps(
            {
                "action": result.action,
                "run_id": result.run.run_id,
                "contract_version": result.contract_version,
                "snapshot_dir": str(result.snapshot_dir),
                "current_pointer_path": str(result.current_pointer_path),
            },
            indent=2,
            sort_keys=True,
        )
    )


def _cmd_export_job_list(args: argparse.Namespace) -> None:
    jobs = _load_export_jobs(args)
    print(
        json.dumps(
            [_serialize_export_job(job) for job in jobs.values()],
            indent=2,
            sort_keys=True,
        )
    )


def _cmd_export_job_run(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    job = _resolve_export_job(args)
    try:
        result = export_job_run_operation(
            store=store,
            state_db=state_db,
            source_run_id=args.run_id,
            job=job,
        )
    except Exception as exc:
        _record_cli_audit_event(
            store,
            action="export_job_run",
            resource_type="export_job",
            resource_id=args.job_name,
            run_id=args.run_id,
            status="failed",
            details={
                "job_name": args.job_name,
                "source_run_id": args.run_id or "",
                "error": str(exc),
            },
        )
        emit_structured_log(
            "export_job_failed",
            component="cli",
            command="export-job-run",
            job_name=args.job_name,
            source_run_id=args.run_id or "",
            duration_seconds=seconds_since(started),
            error=str(exc),
            level="ERROR",
        )
        raise

    _record_cli_audit_event(
        store,
        action="export_job_run",
        resource_type="export_job",
        resource_id=args.job_name,
        run_id=result.source_run.run_id,
        status="reused" if result.action == "reused_completed_export" else "succeeded",
        details={
            "job_name": args.job_name,
            "source_run_id": result.source_run.run_id,
            "export_run_id": result.export_run.export_run_id,
            "snapshot_dir": result.export_run.snapshot_dir,
            "current_pointer_path": result.export_run.current_pointer_path,
            "action": result.action,
        },
    )
    emit_structured_log(
        "export_job_completed",
        component="cli",
        command="export-job-run",
        job_name=args.job_name,
        source_run_id=result.source_run.run_id,
        export_run_id=result.export_run.export_run_id,
        action=result.action,
        duration_seconds=seconds_since(started),
    )

    print(
        json.dumps(
            {
                "action": result.action,
                "job": _serialize_export_job(result.job),
                "export_run": _serialize_export_run_record(result.export_run),
            },
            indent=2,
            sort_keys=True,
        )
    )


def _cmd_export_job_history(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    records = store.list_export_runs(
        job_name=args.job_name,
        source_run_id=args.source_run_id,
        status=args.status,
    )
    print(
        json.dumps(
            [_serialize_export_run_record(record) for record in records],
            indent=2,
            sort_keys=True,
        )
    )


def _cmd_replay_run(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    state_db = _require_state_db(args)
    store = PipelineStateStore(state_db)
    try:
        result = replay_run_operation(
            store=store,
            state_db=state_db,
            source_run_id=args.run_id,
            base_dir=Path(args.base_dir) if args.base_dir else None,
            refresh_mode=args.refresh_mode,
            runner=main,
        )
    except Exception as exc:
        _record_cli_audit_event(
            store,
            action="replay_run",
            resource_type="pipeline_run",
            resource_id=args.run_id or "latest-completed-run",
            run_id=args.run_id,
            status="failed",
            details={
                "base_dir": args.base_dir or "",
                "refresh_mode": args.refresh_mode or "",
                "error": str(exc),
            },
        )
        emit_structured_log(
            "pipeline_run_replay_failed",
            component="cli",
            command="replay-run",
            requested_run_id=args.run_id or "",
            duration_seconds=seconds_since(started),
            error=str(exc),
            level="ERROR",
        )
        raise
    _record_cli_audit_event(
        store,
        action="replay_run",
        resource_type="pipeline_run",
        resource_id=result.result_run.run_id,
        run_id=result.result_run.run_id,
        status="reused" if result.action == "reused_completed_run" else "succeeded",
        details={
            "requested_run_id": result.requested_run.run_id,
            "result_run_id": result.result_run.run_id,
            "refresh_mode": result.refresh_mode,
            "base_dir": str(result.base_dir),
            "replay_command": list(result.replay_command),
            "action": result.action,
        },
    )
    emit_structured_log(
        "pipeline_run_replayed",
        component="cli",
        command="replay-run",
        requested_run_id=result.requested_run.run_id,
        result_run_id=result.result_run.run_id,
        refresh_mode=result.refresh_mode,
        action=result.action,
        duration_seconds=seconds_since(started),
    )
    print(
        json.dumps(
            {
                "action": result.action,
                "requested_run_id": result.requested_run.run_id,
                "result_run_id": result.result_run.run_id,
                "state_db": str(result.state_db),
                "base_dir": str(result.base_dir),
                "refresh_mode": result.refresh_mode,
                "replay_command": list(result.replay_command),
                "source_run": _serialize_run_record(result.requested_run),
                "result_run": _serialize_run_record(result.result_run),
            },
            indent=2,
            sort_keys=True,
        )
    )


def _cmd_serve_api(args: argparse.Namespace) -> None:
    import uvicorn

    state_db = _require_state_db(args)
    runtime_environment = _resolve_runtime_environment(args)
    if runtime_environment is None or runtime_environment.service_auth is None:
        raise ValueError(
            "serve-api requires a runtime environment with service_auth configured via "
            "environment-backed auth settings"
        )
    emit_structured_log(
        "service_api_starting",
        component="cli",
        command="serve-api",
        environment=runtime_environment.name,
        state_db=state_db,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )
    app = create_service_app(
        state_db,
        service_auth=runtime_environment.service_auth,
        config_dir=runtime_environment.config_dir,
        environment=runtime_environment.name,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


def _cmd_report(args: argparse.Namespace) -> None:
    (
        input_file,
        rows,
        decision_counts,
        candidate_pair_count,
        cluster_count,
        golden_record_count,
        review_queue_count,
        summary_updates,
    ) = _collect_report_context(args)
    output_file = Path(args.output)
    _write_quality_outputs(
        output_file,
        input_file,
        rows,
        candidate_pair_count=candidate_pair_count,
        decision_counts=decision_counts,
        cluster_count=cluster_count,
        golden_record_count=golden_record_count,
        review_queue_count=review_queue_count,
        summary_updates=summary_updates,
    )


def _cmd_run_all(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    base = Path(args.base_dir)
    manifest_path = getattr(args, "manifest", None)
    if args.refresh_mode == "incremental":
        if not manifest_path:
            raise ValueError("incremental refresh requires --manifest")
        if not args.state_db:
            raise ValueError("incremental refresh requires --state-db")

    manifest_batch_id = None
    if manifest_path:
        manifest_batch_id = peek_manifest_batch_id(Path(manifest_path))

    state_store = PipelineStateStore(args.state_db) if args.state_db else None
    run_started = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    run_key_args = _resolve_run_key_args(args, batch_id=manifest_batch_id)
    run_key = build_run_key(**run_key_args)
    run_id: str | None = None
    attempt_number = 0
    resume_checkpoint: RunCheckpointRecord | None = None
    resumed_from_run_id: str | None = None
    resumed_from_stage: str | None = None
    latest_checkpoint_run_id: str | None = None
    latest_checkpoint_stage: str | None = None
    checkpoint_duration_baseline = 0.0
    normalized_file = base / "data" / "normalized" / "normalized_person_records.csv"
    matches_file = base / "data" / "matches" / "candidate_scores.csv"
    clusters_file = base / "data" / "matches" / "entity_clusters.csv"
    golden_file = base / "data" / "golden" / "golden_person_records.csv"
    crosswalk_file = base / "data" / "golden" / "source_to_golden_crosswalk.csv"
    review_queue_file = base / "data" / "review_queue" / "manual_review_queue.csv"
    report_file = base / "data" / "exceptions" / "run_report.md"
    phase_metrics: dict[str, dict[str, float | int]] = {}

    emit_structured_log(
        "pipeline_run_requested",
        component="cli",
        command="run-all",
        base_dir=base.resolve(),
        input_mode=run_key_args["input_mode"],
        manifest_path=run_key_args["manifest_path"] or "",
        batch_id=manifest_batch_id or "",
        refresh_mode=args.refresh_mode,
        run_key=run_key,
        state_db=state_store_display_name(args.state_db) if args.state_db else "",
    )

    if state_store is not None:
        start_decision = state_store.begin_run(
            run_key=run_key,
            batch_id=manifest_batch_id,
            input_mode=str(run_key_args["input_mode"]),
            manifest_path=run_key_args["manifest_path"],
            base_dir=str(base.resolve()),
            config_dir=run_key_args["config_dir"],
            profile=run_key_args["profile"],
            seed=run_key_args["seed"],
            formats=run_key_args["formats"],
            started_at_utc=run_started,
        )
        if start_decision.action == "reuse_completed":
            bundle = state_store.load_run_bundle(start_decision.run_id)
            _restore_persisted_run_outputs(base, bundle)
            emit_structured_log(
                "pipeline_run_reused",
                component="cli",
                command="run-all",
                run_id=bundle.run.run_id,
                run_key=run_key,
                duration_seconds=seconds_since(started),
            )
            print(
                f"reused persisted completed run: {state_store_display_name(args.state_db)} "
                f"(run_id={bundle.run.run_id})"
            )
            print("pipeline run complete")
            return
        run_id = start_decision.run_id
        attempt_number = start_decision.attempt_number
        resumed_from_run_id = start_decision.resume_from_run_id
        resumed_from_stage = start_decision.resume_checkpoint_stage
        if resumed_from_run_id and resumed_from_stage:
            resume_checkpoint = state_store.load_run_checkpoint(
                run_id=resumed_from_run_id,
                stage_name=resumed_from_stage,
            )
            latest_checkpoint_run_id = resume_checkpoint.run_id
            latest_checkpoint_stage = resume_checkpoint.stage_name
            checkpoint_duration_baseline = resume_checkpoint.total_duration_seconds
            phase_metrics = _deep_copy_phase_metrics(resume_checkpoint.phase_metrics)
            emit_structured_log(
                "pipeline_run_resuming",
                component="cli",
                command="run-all",
                run_id=run_id,
                run_key=run_key,
                resume_from_run_id=resumed_from_run_id,
                resume_from_stage=resumed_from_stage,
                resume_checkpointed_at_utc=start_decision.resume_checkpointed_at_utc or "",
            )
        emit_structured_log(
            "pipeline_run_started",
            component="cli",
            command="run-all",
            run_id=run_id,
            run_key=run_key,
            attempt_number=attempt_number,
            input_mode=run_key_args["input_mode"],
            refresh_mode=args.refresh_mode,
            resume_from_run_id=resumed_from_run_id or "",
            resume_from_stage=resumed_from_stage or "",
        )

    input_mode = "manifest" if manifest_path else "synthetic"
    normalized_rows: list[dict[str, str]] = []
    match_rows: list[dict[str, str | float]] = []
    blocking_metrics_rows: list[dict[str, str | int]] = []
    clustered_rows: list[dict[str, str]] = []
    golden_rows: list[dict[str, str]] = []
    crosswalk_rows: list[dict[str, str]] = []
    review_rows: list[dict[str, str | float]] = []
    active_review_rows: list[dict[str, str | float]] = []
    run_context: dict[str, object] = {
        "input_mode": input_mode,
        "batch_id": manifest_batch_id or "",
        "manifest_path": str(Path(manifest_path).resolve()) if manifest_path else "",
        "replay_source_run_id": getattr(args, "replay_source_run_id", None) or "",
        "config_fingerprint": "",
        "refresh_mode": args.refresh_mode,
        "profile": None if manifest_path else args.profile,
        "seed": None if manifest_path else args.seed,
        "person_count": None,
        "formats": None,
    }
    refresh_summary: dict[str, object] = {
        "mode": args.refresh_mode,
        "fallback_to_full": args.refresh_mode != "incremental",
        "predecessor_run_id": None,
        "affected_record_count": 0,
        "reused_record_count": 0,
        "inserted_record_count": 0,
        "changed_record_count": 0,
        "removed_record_count": 0,
        "recalculated_candidate_pair_count": 0,
        "reused_candidate_pair_count": 0,
        "recalculated_cluster_count": 0,
        "reused_cluster_count": 0,
    }

    try:
        config = _load_config(args.config_dir, args.environment)
        resolved_manifest: ResolvedBatchManifest | None = None
        generation_result = None
        batch_id: str | None = None
        formats_value: str | None = None
        generated_person_count: int | None = None
        config_fingerprint = _config_fingerprint(config)
        run_context: dict[str, object] = {
            "input_mode": input_mode,
            "batch_id": manifest_batch_id or "",
            "manifest_path": str(Path(manifest_path).resolve()) if manifest_path else "",
            "replay_source_run_id": getattr(args, "replay_source_run_id", None) or "",
            "config_fingerprint": config_fingerprint,
            "refresh_mode": args.refresh_mode,
            "profile": None if manifest_path else args.profile,
            "seed": None if manifest_path else args.seed,
            "person_count": None,
            "formats": None,
        }
        refresh_summary: dict[str, object] = {
            "mode": args.refresh_mode,
            "fallback_to_full": args.refresh_mode != "incremental",
            "predecessor_run_id": None,
            "affected_record_count": 0,
            "reused_record_count": 0,
            "inserted_record_count": 0,
            "changed_record_count": 0,
            "removed_record_count": 0,
            "recalculated_candidate_pair_count": 0,
            "reused_candidate_pair_count": 0,
            "recalculated_cluster_count": 0,
            "reused_cluster_count": 0,
        }
        normalized_rows: list[dict[str, str]] = []
        match_rows: list[dict[str, str | float]] = []
        blocking_metrics_rows: list[dict[str, str | int]] = []
        clustered_rows: list[dict[str, str]] = []
        golden_rows: list[dict[str, str]] = []
        crosswalk_rows: list[dict[str, str]] = []
        review_rows: list[dict[str, str | float]] = []
        active_review_rows: list[dict[str, str | float]] = []
        resume_stage_order = 0

        if resume_checkpoint is not None:
            resume_stage_order = RUN_CHECKPOINT_STAGE_ORDER[resume_checkpoint.stage_name]
            checkpoint_run_context = resume_checkpoint.summary.get("run_context", {})
            if isinstance(checkpoint_run_context, dict):
                checkpoint_person_count = checkpoint_run_context.get("person_count")
                if checkpoint_person_count not in (None, ""):
                    generated_person_count = int(checkpoint_person_count)
                checkpoint_formats = checkpoint_run_context.get("formats")
                if checkpoint_formats not in (None, ""):
                    formats_value = str(checkpoint_formats)
            checkpoint_refresh = resume_checkpoint.summary.get("refresh", {})
            if isinstance(checkpoint_refresh, dict):
                refresh_summary.update(checkpoint_refresh)
            _restore_checkpoint_outputs(
                normalized_file=normalized_file,
                matches_file=matches_file,
                clusters_file=clusters_file,
                golden_file=golden_file,
                crosswalk_file=crosswalk_file,
                review_queue_file=review_queue_file,
                checkpoint=resume_checkpoint,
            )
            normalized_rows = [dict(row) for row in resume_checkpoint.normalized_rows]
            match_rows = [dict(row) for row in resume_checkpoint.match_rows]
            blocking_metrics_rows = [dict(row) for row in resume_checkpoint.blocking_metrics_rows]
            clustered_rows = [dict(row) for row in resume_checkpoint.cluster_rows]
            golden_rows = [dict(row) for row in resume_checkpoint.golden_rows]
            crosswalk_rows = [dict(row) for row in resume_checkpoint.crosswalk_rows]
            review_rows = [dict(row) for row in resume_checkpoint.review_rows]
            active_review_rows = [dict(row) for row in resume_checkpoint.active_review_rows]
            print(
                f"resuming persisted run from checkpoint: {resume_checkpoint.stage_name} "
                f"(source_run_id={resume_checkpoint.run_id})"
            )

        if manifest_path:
            batch_id = manifest_batch_id
            if resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["match"]:
                resolved_manifest = _resolve_manifest_inputs(manifest_path)
                print(
                    f"validated batch manifest: {resolved_manifest.manifest_path} "
                    f"(batch_id={resolved_manifest.manifest.batch_id})"
                )
                for path in resolved_manifest.input_paths:
                    print(f" - {path}")
            if resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["normalize"]:
                assert resolved_manifest is not None
                normalize_started = time.perf_counter()
                manifest_rows = resolved_manifest.all_rows()
                batch_id = resolved_manifest.manifest.batch_id
                normalized_rows = _write_normalized_output(
                    normalized_file,
                    manifest_rows,
                    config,
                )
                phase_metrics["normalize"] = _build_phase_metric(
                    seconds_since(normalize_started),
                    input_record_count=len(manifest_rows),
                    output_record_count=len(normalized_rows),
                )
        else:
            formats = _parse_formats(args.formats)
            formats_value = formats_value or ",".join(formats)
            synthetic_dir = base / "data" / "synthetic_sources"
            if resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["normalize"]:
                generate_started = time.perf_counter()
                generation_result = generate_synthetic_sources(
                    output_dir=synthetic_dir,
                    profile=args.profile,
                    seed=args.seed,
                    duplicate_rate=args.duplicate_rate,
                    formats=formats,
                    person_count_override=args.person_count,
                )
                generated_person_count = int(generation_result.summary["person_entity_count"])
                phase_metrics["generate"] = _build_phase_metric(
                    seconds_since(generate_started),
                    output_record_count=int(generation_result.summary["source_a_record_count"])
                    + int(generation_result.summary["source_b_record_count"]),
                )
                normalize_input_rows = _read_rows(
                    _resolve_generated_person_input_paths(synthetic_dir, formats=formats)
                )
                normalize_started = time.perf_counter()
                normalized_rows = _write_normalized_output(
                    normalized_file,
                    normalize_input_rows,
                    config,
                )
                phase_metrics["normalize"] = _build_phase_metric(
                    seconds_since(normalize_started),
                    input_record_count=len(normalize_input_rows),
                    output_record_count=len(normalized_rows),
                )
            elif generated_person_count is None:
                generated_person_count = len(
                    {
                        row.get("person_entity_id", "")
                        for row in normalized_rows
                        if row.get("person_entity_id")
                    }
                )
            batch_id = f"synthetic:{args.profile}:{args.seed}"
            if args.person_count is not None and generated_person_count is not None:
                batch_id = f"{batch_id}:{generated_person_count}"

        run_context["batch_id"] = batch_id or ""
        run_context["person_count"] = generated_person_count
        run_context["formats"] = formats_value

        def persist_checkpoint(stage_name: str) -> None:
            nonlocal latest_checkpoint_run_id
            nonlocal latest_checkpoint_stage
            if state_store is None or run_id is None:
                return
            checkpoint_summary = _build_pipeline_summary(
                normalized_rows=normalized_rows,
                match_rows=match_rows,
                clustered_rows=clustered_rows,
                golden_rows=golden_rows,
                active_review_rows=active_review_rows,
                run_context=run_context,
                refresh_summary=refresh_summary,
                resume_summary=_build_resume_summary(
                    resumed_from_run_id=resumed_from_run_id,
                    resumed_from_stage=resumed_from_stage,
                    available_checkpoint_run_id=run_id,
                    available_checkpoint_stage=stage_name,
                ),
                phase_metrics=phase_metrics,
                total_duration_seconds=checkpoint_duration_baseline + seconds_since(started),
            )
            state_store.persist_run_checkpoint(
                run_id=run_id,
                run_key=run_key,
                attempt_number=attempt_number,
                stage_name=stage_name,
                checkpointed_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                total_duration_seconds=checkpoint_duration_baseline + seconds_since(started),
                phase_metrics=_deep_copy_phase_metrics(phase_metrics),
                normalized_rows=normalized_rows,
                match_rows=match_rows,
                blocking_metrics_rows=blocking_metrics_rows,
                cluster_rows=clustered_rows,
                golden_rows=golden_rows,
                crosswalk_rows=crosswalk_rows,
                review_rows=review_rows,
                active_review_rows=active_review_rows,
                summary=checkpoint_summary,
            )
            latest_checkpoint_run_id = run_id
            latest_checkpoint_stage = stage_name

        if resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["normalize"]:
            persist_checkpoint("normalize")

        review_source_bundle = None
        review_overrides: dict[tuple[str, str], str] = {}
        forced_review_pairs: set[tuple[str, str]] = set()
        if (
            state_store is not None
            and resolved_manifest is not None
            and resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["match"]
        ):
            replay_source_run_id = getattr(args, "replay_source_run_id", None)
            if replay_source_run_id:
                review_source_run = state_store.load_run_record(replay_source_run_id)
                if review_source_run.status != "completed":
                    raise ValueError(
                        f"Replay source run must be completed, received status={review_source_run.status!r} "
                        f"for run_id={review_source_run.run_id}"
                    )
            else:
                review_source_run = state_store.latest_completed_run_for_manifest(
                    manifest_path=str(resolved_manifest.manifest_path),
                    config_dir=str(Path(args.config_dir).resolve()) if args.config_dir else None,
                )
            if review_source_run is not None:
                review_source_bundle = state_store.load_run_bundle(review_source_run.run_id)
                review_overrides = build_review_override_map(review_source_bundle.review_rows)
                current_record_ids = {
                    str(row.get("source_record_id", "")).strip()
                    for row in normalized_rows
                    if str(row.get("source_record_id", "")).strip()
                }
                forced_review_pairs = {
                    pair
                    for pair in review_overrides
                    if pair[0] in current_record_ids and pair[1] in current_record_ids
                }

        previous_bundle = None
        if (
            args.refresh_mode == "incremental"
            and state_store is not None
            and resolved_manifest is not None
            and resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["match"]
        ):
            previous_run = review_source_bundle.run if review_source_bundle is not None else None
            if previous_run is not None:
                previous_bundle = review_source_bundle
                previous_config_fingerprint = str(
                    previous_bundle.run.summary.get("run_context", {}).get("config_fingerprint", "")
                )
                if previous_config_fingerprint and previous_config_fingerprint == config_fingerprint:
                    incremental_result = refresh_incremental_run(
                        current_rows=normalized_rows,
                        previous_bundle=previous_bundle,
                        config=config,
                    )
                    match_rows = incremental_result.match_rows
                    blocking_metrics_rows = incremental_result.blocking_metrics_rows
                    clustered_rows = incremental_result.clustered_rows
                    golden_rows = incremental_result.golden_rows
                    crosswalk_rows = incremental_result.crosswalk_rows
                    active_review_rows = incremental_result.active_review_rows
                    review_rows = incremental_result.review_rows
                    refresh_summary = incremental_result.metadata
                    match_started = time.perf_counter()
                    _write_precomputed_match_outputs(matches_file, match_rows, blocking_metrics_rows)
                    phase_metrics["match"] = _build_phase_metric(
                        seconds_since(match_started),
                        input_record_count=len(normalized_rows),
                        output_record_count=len(match_rows),
                        candidate_pair_count=len(match_rows),
                    )
                    cluster_started = time.perf_counter()
                    _write_precomputed_cluster_output(
                        clusters_file,
                        _build_cluster_output_rows(clustered_rows),
                    )
                    phase_metrics["cluster"] = _build_phase_metric(
                        seconds_since(cluster_started),
                        input_record_count=len(normalized_rows),
                        output_record_count=len(clustered_rows),
                    )
                    review_queue_started = time.perf_counter()
                    _write_precomputed_review_queue_output(review_queue_file, active_review_rows)
                    phase_metrics["review_queue"] = _build_phase_metric(
                        seconds_since(review_queue_started),
                        input_record_count=len(match_rows),
                        output_record_count=len(active_review_rows),
                    )
                    golden_started = time.perf_counter()
                    _write_precomputed_golden_output(golden_file, golden_rows)
                    phase_metrics["golden"] = _build_phase_metric(
                        seconds_since(golden_started),
                        input_record_count=len(clustered_rows),
                        output_record_count=len(golden_rows),
                    )
                    crosswalk_started = time.perf_counter()
                    _write_precomputed_crosswalk_output(crosswalk_file, crosswalk_rows)
                    phase_metrics["crosswalk"] = _build_phase_metric(
                        seconds_since(crosswalk_started),
                        input_record_count=len(clustered_rows),
                        output_record_count=len(crosswalk_rows),
                    )
                else:
                    refresh_summary.update(
                        {
                            "fallback_to_full": True,
                            "predecessor_run_id": previous_run.run_id,
                        }
                    )
                    if previous_config_fingerprint and previous_config_fingerprint != config_fingerprint:
                        print(
                            "incremental refresh fell back to full rebuild: config fingerprint changed "
                            f"({previous_config_fingerprint} -> {config_fingerprint})"
                        )
                    else:
                        print(
                            "incremental refresh fell back to full rebuild: predecessor run is missing "
                            "a compatible config fingerprint"
                        )
            else:
                refresh_summary["fallback_to_full"] = True
                print("incremental refresh fell back to full rebuild: no prior completed manifest run found")

        if resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["match"]:
            if (
                args.refresh_mode == "incremental"
                and previous_bundle is not None
                and not bool(refresh_summary.get("fallback_to_full", False))
            ):
                match_started = time.perf_counter()
                _write_precomputed_match_outputs(matches_file, match_rows, blocking_metrics_rows)
                phase_metrics["match"] = _build_phase_metric(
                    seconds_since(match_started),
                    input_record_count=len(normalized_rows),
                    output_record_count=len(match_rows),
                    candidate_pair_count=len(match_rows),
                )
                persist_checkpoint("match")

                cluster_started = time.perf_counter()
                _write_precomputed_cluster_output(
                    clusters_file,
                    _build_cluster_output_rows(clustered_rows),
                )
                phase_metrics["cluster"] = _build_phase_metric(
                    seconds_since(cluster_started),
                    input_record_count=len(normalized_rows),
                    output_record_count=len(clustered_rows),
                )
                persist_checkpoint("cluster")

                review_queue_started = time.perf_counter()
                _write_precomputed_review_queue_output(review_queue_file, review_rows)
                phase_metrics["review_queue"] = _build_phase_metric(
                    seconds_since(review_queue_started),
                    input_record_count=len(match_rows),
                    output_record_count=len(active_review_rows),
                )
                persist_checkpoint("review_queue")

                golden_started = time.perf_counter()
                _write_precomputed_golden_output(golden_file, golden_rows)
                phase_metrics["golden"] = _build_phase_metric(
                    seconds_since(golden_started),
                    input_record_count=len(clustered_rows),
                    output_record_count=len(golden_rows),
                )
                persist_checkpoint("golden")

                crosswalk_started = time.perf_counter()
                _write_precomputed_crosswalk_output(crosswalk_file, crosswalk_rows)
                phase_metrics["crosswalk"] = _build_phase_metric(
                    seconds_since(crosswalk_started),
                    input_record_count=len(clustered_rows),
                    output_record_count=len(crosswalk_rows),
                )
                persist_checkpoint("crosswalk")
            else:
                match_started = time.perf_counter()
                match_rows, blocking_metrics_rows = _write_match_output(
                    matches_file,
                    normalized_rows,
                    config,
                    forced_pairs=forced_review_pairs,
                    review_overrides=review_overrides,
                )
                phase_metrics["match"] = _build_phase_metric(
                    seconds_since(match_started),
                    input_record_count=len(normalized_rows),
                    output_record_count=len(match_rows),
                    candidate_pair_count=len(match_rows),
                )
                persist_checkpoint("match")

                cluster_started = time.perf_counter()
                clustered_rows = _write_cluster_output(clusters_file, normalized_rows, match_rows)
                phase_metrics["cluster"] = _build_phase_metric(
                    seconds_since(cluster_started),
                    input_record_count=len(normalized_rows),
                    output_record_count=len(clustered_rows),
                )
                persist_checkpoint("cluster")

                review_queue_started = time.perf_counter()
                active_review_rows, review_rows = _write_manual_review_output(
                    review_queue_file,
                    match_rows,
                    previous_review_rows=review_source_bundle.review_rows if review_source_bundle is not None else None,
                )
                phase_metrics["review_queue"] = _build_phase_metric(
                    seconds_since(review_queue_started),
                    input_record_count=len(match_rows),
                    output_record_count=len(active_review_rows),
                )
                persist_checkpoint("review_queue")

                golden_started = time.perf_counter()
                golden_rows = _write_golden_output(golden_file, clustered_rows, config)
                phase_metrics["golden"] = _build_phase_metric(
                    seconds_since(golden_started),
                    input_record_count=len(clustered_rows),
                    output_record_count=len(golden_rows),
                )
                persist_checkpoint("golden")

                crosswalk_started = time.perf_counter()
                crosswalk_rows = _write_crosswalk_output(crosswalk_file, clustered_rows, golden_rows)
                phase_metrics["crosswalk"] = _build_phase_metric(
                    seconds_since(crosswalk_started),
                    input_record_count=len(clustered_rows),
                    output_record_count=len(crosswalk_rows),
                )
                persist_checkpoint("crosswalk")

                if args.refresh_mode == "full":
                    refresh_summary["fallback_to_full"] = False
                    refresh_summary["reused_record_count"] = 0
                    refresh_summary["affected_record_count"] = len(normalized_rows)
                    refresh_summary["recalculated_candidate_pair_count"] = len(match_rows)
                    refresh_summary["recalculated_cluster_count"] = _cluster_count(clustered_rows)
                else:
                    refresh_summary["affected_record_count"] = len(normalized_rows)
                    refresh_summary["recalculated_candidate_pair_count"] = len(match_rows)
                    refresh_summary["recalculated_cluster_count"] = _cluster_count(clustered_rows)
                    refresh_summary["reused_record_count"] = 0

        _write_public_safety_demo_outputs(base_dir=base, missing_ok=True)

        resume_summary = _build_resume_summary(
            resumed_from_run_id=resumed_from_run_id,
            resumed_from_stage=resumed_from_stage,
            available_checkpoint_run_id=latest_checkpoint_run_id,
            available_checkpoint_stage=latest_checkpoint_stage,
        )
        report_started = time.perf_counter()
        summary = _write_quality_outputs(
            report_file,
            normalized_file,
            normalized_rows,
            candidate_pair_count=len(match_rows),
            decision_counts=_decision_counts(match_rows),
            cluster_count=_cluster_count(clustered_rows),
            golden_record_count=len(golden_rows),
            review_queue_count=len(active_review_rows),
            summary_updates={
                "run_context": run_context,
                "refresh": refresh_summary,
                "resume": resume_summary,
                "performance": {
                    "total_duration_seconds": round(
                        checkpoint_duration_baseline + seconds_since(started),
                        6,
                    ),
                    "phase_metrics": _deep_copy_phase_metrics(phase_metrics),
                },
            },
        )
        phase_metrics["report"] = _build_phase_metric(
            seconds_since(report_started),
            input_record_count=len(normalized_rows),
            output_record_count=1,
        )
        summary["performance"] = {
            "total_duration_seconds": round(checkpoint_duration_baseline + seconds_since(started), 6),
            "phase_metrics": _deep_copy_phase_metrics(phase_metrics),
        }
        _write_run_summary_artifacts(
            report_file=report_file,
            normalized_file=normalized_file,
            summary=summary,
        )
        persist_checkpoint("report")
        if state_store is not None and run_id is not None:
            persist_started = time.perf_counter()
            state_store.persist_run(
                metadata=PersistRunMetadata(
                    run_id=run_id,
                    run_key=run_key,
                    attempt_number=attempt_number,
                    batch_id=batch_id,
                    input_mode=input_mode,
                    manifest_path=str(Path(manifest_path).resolve()) if manifest_path else None,
                    base_dir=str(base.resolve()),
                    config_dir=str(Path(args.config_dir).resolve()) if args.config_dir else None,
                    profile=None if manifest_path else args.profile,
                    seed=None if manifest_path else args.seed,
                    formats=formats_value,
                    started_at_utc=run_started,
                    finished_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    status="completed",
                ),
                normalized_rows=normalized_rows,
                match_rows=match_rows,
                blocking_metrics_rows=blocking_metrics_rows,
                cluster_rows=read_dict_rows(clusters_file),
                golden_rows=golden_rows,
                crosswalk_rows=crosswalk_rows,
                review_rows=review_rows,
                summary=summary,
            )
            phase_metrics["persist_state"] = _build_phase_metric(
                seconds_since(persist_started),
                input_record_count=len(normalized_rows),
                output_record_count=1,
            )
            summary["performance"] = {
                "total_duration_seconds": round(checkpoint_duration_baseline + seconds_since(started), 6),
                "phase_metrics": _deep_copy_phase_metrics(phase_metrics),
            }
            state_store.update_run_summary(run_id=run_id, summary=summary)
            _write_run_summary_artifacts(
                report_file=report_file,
                normalized_file=normalized_file,
                summary=summary,
            )
            if manifest_path:
                if resolved_manifest is None:
                    resolved_manifest = _resolve_manifest_inputs(manifest_path)
                archive_started = time.perf_counter()
                bundle_root = replay_bundle_root_for_run(base_dir=base, run_id=run_id)
                try:
                    verification = archive_replay_bundle(
                        run_id=run_id,
                        base_dir=base,
                        resolved_manifest=resolved_manifest,
                    )
                    summary["replay_bundle"] = replay_bundle_summary_from_result(verification)
                    emit_structured_log(
                        "replay_bundle_archived",
                        component="cli",
                        command="run-all",
                        run_id=run_id,
                        bundle_manifest_path=verification.bundle_manifest_path,
                        recoverable=verification.recoverable,
                        artifact_count=verification.artifact_count,
                    )
                    print(f"archived replay bundle: {verification.bundle_manifest_path}")
                except Exception as exc:
                    summary["replay_bundle"] = {
                        "bundle_id": f"replay-bundle-{run_id}",
                        "bundle_version": REPLAY_BUNDLE_VERSION,
                        "status": "failed",
                        "recoverable": False,
                        "replayable_from_bundle": False,
                        "restore_mode": REPLAY_BUNDLE_RESTORE_MODE,
                        "bundle_root": str(bundle_root),
                        "bundle_manifest_path": str(bundle_root / "bundle_manifest.json"),
                        "original_manifest_path": "",
                        "replay_manifest_path": "",
                        "landing_snapshot_root": str(bundle_root / "landing_snapshot"),
                        "created_at_utc": replay_bundle_utc_now(),
                        "verified_at_utc": "",
                        "artifact_count": 0,
                        "source_count": 0,
                        "total_bytes": 0,
                        "verification_errors": [str(exc)],
                        "artifacts": [],
                    }
                    emit_structured_log(
                        "replay_bundle_archive_failed",
                        component="cli",
                        command="run-all",
                        run_id=run_id,
                        bundle_manifest_path=bundle_root / "bundle_manifest.json",
                        duration_seconds=seconds_since(archive_started),
                        error=str(exc),
                        level="ERROR",
                    )
                    print(f"replay bundle unavailable: {exc}")
                phase_metrics["archive_replay_bundle"] = _build_phase_metric(
                    seconds_since(archive_started),
                    input_record_count=len(normalized_rows),
                    output_record_count=1,
                )
                summary["performance"] = {
                    "total_duration_seconds": round(checkpoint_duration_baseline + seconds_since(started), 6),
                    "phase_metrics": _deep_copy_phase_metrics(phase_metrics),
                }
                state_store.update_run_summary(run_id=run_id, summary=summary)
                _write_run_summary_artifacts(
                    report_file=report_file,
                    normalized_file=normalized_file,
                    summary=summary,
                )
            print(f"persisted run state: {state_store_display_name(args.state_db)} (run_id={run_id})")
        emit_structured_log(
            "pipeline_run_completed",
            component="cli",
            command="run-all",
            run_id=run_id or "",
            run_key=run_key,
            input_mode=input_mode,
            total_records=summary["total_records"],
            candidate_pair_count=summary["candidate_pair_count"],
            cluster_count=summary["cluster_count"],
            golden_record_count=summary["golden_record_count"],
            review_queue_count=summary["review_queue_count"],
            duration_seconds=summary["performance"]["total_duration_seconds"],
        )
        print("pipeline run complete")
    except Exception as exc:
        if state_store is not None and run_id is not None:
            failure_summary = None
            if normalized_rows:
                failure_summary = _build_pipeline_summary(
                    normalized_rows=normalized_rows,
                    match_rows=match_rows,
                    clustered_rows=clustered_rows,
                    golden_rows=golden_rows,
                    active_review_rows=active_review_rows,
                    run_context=run_context,
                    refresh_summary=refresh_summary,
                    resume_summary=_build_resume_summary(
                        resumed_from_run_id=resumed_from_run_id,
                        resumed_from_stage=resumed_from_stage,
                        available_checkpoint_run_id=latest_checkpoint_run_id,
                        available_checkpoint_stage=latest_checkpoint_stage,
                    ),
                    phase_metrics=phase_metrics,
                    total_duration_seconds=checkpoint_duration_baseline + seconds_since(started),
                )
            elif latest_checkpoint_run_id and latest_checkpoint_stage:
                checkpoint_summary = state_store.load_run_checkpoint(
                    run_id=latest_checkpoint_run_id,
                    stage_name=latest_checkpoint_stage,
                ).summary
                if isinstance(checkpoint_summary, dict):
                    failure_summary = dict(checkpoint_summary)
                    failure_summary["resume"] = _build_resume_summary(
                        resumed_from_run_id=resumed_from_run_id,
                        resumed_from_stage=resumed_from_stage,
                        available_checkpoint_run_id=latest_checkpoint_run_id,
                        available_checkpoint_stage=latest_checkpoint_stage,
                    )
            if failure_summary is not None:
                failure_summary["failure"] = {"detail": str(exc)}
            state_store.mark_run_failed(
                run_id=run_id,
                finished_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                failure_detail=str(exc),
                summary=failure_summary,
            )
        emit_structured_log(
            "pipeline_run_failed",
            component="cli",
            command="run-all",
            run_id=run_id or "",
            run_key=run_key,
            input_mode=run_key_args["input_mode"],
            refresh_mode=args.refresh_mode,
            duration_seconds=seconds_since(started),
            error=str(exc),
            level="ERROR",
        )
        raise


def _cmd_stream_refresh(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    base = Path(args.base_dir)
    state_db = _require_state_db(args)
    state_store = PipelineStateStore(state_db)
    source_run_id = args.source_run_id or state_store.latest_completed_run_id()
    if source_run_id is None:
        raise FileNotFoundError(f"No completed persisted runs found in {state_store_display_name(state_db)}")

    source_run = state_store.load_run_record(source_run_id)
    if source_run.status != "completed":
        raise ValueError(
            f"stream-refresh requires a completed predecessor run, received status={source_run.status!r} "
            f"for run_id={source_run.run_id}"
        )
    source_bundle = state_store.load_run_bundle(source_run_id)
    resolved_batch = resolve_stream_event_batch(Path(args.events), default_stream_id=args.stream_id)

    source_run_context = source_run.summary.get("run_context", {})
    if not isinstance(source_run_context, dict):
        source_run_context = {}

    run_started = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    run_key = build_run_key(**_resolve_stream_run_key_args(args, source_run_id=source_run_id, batch=resolved_batch))
    run_id: str | None = None
    attempt_number = 0
    resume_checkpoint: RunCheckpointRecord | None = None
    resumed_from_run_id: str | None = None
    resumed_from_stage: str | None = None
    latest_checkpoint_run_id: str | None = None
    latest_checkpoint_stage: str | None = None
    checkpoint_duration_baseline = 0.0
    phase_metrics: dict[str, dict[str, float | int]] = {}

    normalized_file = base / "data" / "normalized" / "normalized_person_records.csv"
    matches_file = base / "data" / "matches" / "candidate_scores.csv"
    clusters_file = base / "data" / "matches" / "entity_clusters.csv"
    golden_file = base / "data" / "golden" / "golden_person_records.csv"
    crosswalk_file = base / "data" / "golden" / "source_to_golden_crosswalk.csv"
    review_queue_file = base / "data" / "review_queue" / "manual_review_queue.csv"
    report_file = base / "data" / "exceptions" / "run_report.md"
    stream_events_file = base / "data" / "events" / "stream_events.jsonl"

    emit_structured_log(
        "stream_refresh_requested",
        component="cli",
        command="stream-refresh",
        base_dir=base.resolve(),
        source_run_id=source_run_id,
        stream_id=resolved_batch.stream_id,
        batch_id=resolved_batch.batch_id,
        event_path=resolved_batch.event_path,
        event_count=len(resolved_batch.events),
        run_key=run_key,
        state_db=state_store_display_name(state_db),
    )

    start_decision = state_store.begin_run(
        run_key=run_key,
        batch_id=resolved_batch.batch_id,
        input_mode="event_stream",
        manifest_path=str(resolved_batch.event_path),
        base_dir=str(base.resolve()),
        config_dir=str(Path(args.config_dir).resolve()) if args.config_dir else None,
        profile=None,
        seed=None,
        formats="jsonl",
        started_at_utc=run_started,
    )
    if start_decision.action == "reuse_completed":
        bundle = state_store.load_run_bundle(start_decision.run_id)
        _restore_persisted_run_outputs(base, bundle)
        emit_structured_log(
            "stream_refresh_reused",
            component="cli",
            command="stream-refresh",
            run_id=bundle.run.run_id,
            source_run_id=source_run_id,
            stream_id=resolved_batch.stream_id,
            duration_seconds=seconds_since(started),
        )
        print(
            json.dumps(
                {
                    "action": "reused_completed_run",
                    "run_id": bundle.run.run_id,
                    "source_run_id": source_run_id,
                    "stream_id": resolved_batch.stream_id,
                    "state_db": state_store_display_name(state_db),
                },
                sort_keys=True,
            )
        )
        return

    run_id = start_decision.run_id
    attempt_number = start_decision.attempt_number
    resumed_from_run_id = start_decision.resume_from_run_id
    resumed_from_stage = start_decision.resume_checkpoint_stage
    if resumed_from_run_id and resumed_from_stage:
        resume_checkpoint = state_store.load_run_checkpoint(
            run_id=resumed_from_run_id,
            stage_name=resumed_from_stage,
        )
        latest_checkpoint_run_id = resume_checkpoint.run_id
        latest_checkpoint_stage = resume_checkpoint.stage_name
        checkpoint_duration_baseline = resume_checkpoint.total_duration_seconds
        phase_metrics = _deep_copy_phase_metrics(resume_checkpoint.phase_metrics)
        emit_structured_log(
            "stream_refresh_resuming",
            component="cli",
            command="stream-refresh",
            run_id=run_id,
            source_run_id=source_run_id,
            stream_id=resolved_batch.stream_id,
            resume_from_run_id=resumed_from_run_id,
            resume_from_stage=resumed_from_stage,
            resume_checkpointed_at_utc=start_decision.resume_checkpointed_at_utc or "",
        )
    emit_structured_log(
        "stream_refresh_started",
        component="cli",
        command="stream-refresh",
        run_id=run_id,
        source_run_id=source_run_id,
        stream_id=resolved_batch.stream_id,
        batch_id=resolved_batch.batch_id,
        attempt_number=attempt_number,
        resume_from_run_id=resumed_from_run_id or "",
        resume_from_stage=resumed_from_stage or "",
    )

    config = _load_config(args.config_dir, args.environment)
    config_fingerprint = _config_fingerprint(config)
    normalized_rows: list[dict[str, str]] = []
    match_rows: list[dict[str, str | float]] = []
    blocking_metrics_rows: list[dict[str, str | int]] = []
    clustered_rows: list[dict[str, str]] = []
    golden_rows: list[dict[str, str]] = []
    crosswalk_rows: list[dict[str, str]] = []
    review_rows: list[dict[str, str | float]] = []
    active_review_rows: list[dict[str, str | float]] = []
    run_context: dict[str, object] = {
        "input_mode": "event_stream",
        "batch_id": resolved_batch.batch_id,
        "manifest_path": str(resolved_batch.event_path),
        "replay_source_run_id": "",
        "config_fingerprint": config_fingerprint,
        "refresh_mode": "incremental",
        "profile": source_run_context.get("profile"),
        "seed": source_run_context.get("seed"),
        "person_count": source_run_context.get("person_count"),
        "formats": "jsonl",
        "source_run_id": source_run_id,
        "source_input_mode": source_run.input_mode,
        "stream_id": resolved_batch.stream_id,
    }
    refresh_summary: dict[str, object] = {
        "mode": "incremental",
        "fallback_to_full": False,
        "predecessor_run_id": source_run_id,
        "affected_record_count": 0,
        "reused_record_count": 0,
        "inserted_record_count": 0,
        "changed_record_count": 0,
        "removed_record_count": 0,
        "recalculated_candidate_pair_count": 0,
        "reused_candidate_pair_count": 0,
        "recalculated_cluster_count": 0,
        "reused_cluster_count": 0,
    }
    stream_summary: dict[str, object] = {
        "mode": "event_stream",
        "stream_id": resolved_batch.stream_id,
        "batch_id": resolved_batch.batch_id,
        "source_run_id": source_run_id,
        "event_path": str(resolved_batch.event_path),
        "event_snapshot_path": str(stream_events_file),
        "event_count": len(resolved_batch.events),
        "upsert_count": 0,
        "delete_count": 0,
        "first_sequence": resolved_batch.first_sequence,
        "last_sequence": resolved_batch.last_sequence,
        "event_sha256": resolved_batch.content_sha256,
    }
    resume_stage_order = 0

    try:
        if resume_checkpoint is not None:
            resume_stage_order = RUN_CHECKPOINT_STAGE_ORDER[resume_checkpoint.stage_name]
            checkpoint_run_context = resume_checkpoint.summary.get("run_context", {})
            if isinstance(checkpoint_run_context, dict):
                run_context.update(checkpoint_run_context)
            checkpoint_refresh = resume_checkpoint.summary.get("refresh", {})
            if isinstance(checkpoint_refresh, dict):
                refresh_summary.update(checkpoint_refresh)
            checkpoint_stream = resume_checkpoint.summary.get("stream", {})
            if isinstance(checkpoint_stream, dict):
                stream_summary.update(checkpoint_stream)
            _restore_checkpoint_outputs(
                normalized_file=normalized_file,
                matches_file=matches_file,
                clusters_file=clusters_file,
                golden_file=golden_file,
                crosswalk_file=crosswalk_file,
                review_queue_file=review_queue_file,
                checkpoint=resume_checkpoint,
            )
            normalized_rows = [dict(row) for row in resume_checkpoint.normalized_rows]
            match_rows = [dict(row) for row in resume_checkpoint.match_rows]
            blocking_metrics_rows = [dict(row) for row in resume_checkpoint.blocking_metrics_rows]
            clustered_rows = [dict(row) for row in resume_checkpoint.cluster_rows]
            golden_rows = [dict(row) for row in resume_checkpoint.golden_rows]
            crosswalk_rows = [dict(row) for row in resume_checkpoint.crosswalk_rows]
            review_rows = [dict(row) for row in resume_checkpoint.review_rows]
            active_review_rows = [dict(row) for row in resume_checkpoint.active_review_rows]
            print(
                f"resuming stream refresh from checkpoint: {resume_checkpoint.stage_name} "
                f"(source_run_id={source_run_id})"
            )

        def persist_checkpoint(stage_name: str) -> None:
            nonlocal latest_checkpoint_run_id
            nonlocal latest_checkpoint_stage
            assert run_id is not None
            checkpoint_summary = _build_pipeline_summary(
                normalized_rows=normalized_rows,
                match_rows=match_rows,
                clustered_rows=clustered_rows,
                golden_rows=golden_rows,
                active_review_rows=active_review_rows,
                run_context=run_context,
                refresh_summary=refresh_summary,
                resume_summary=_build_resume_summary(
                    resumed_from_run_id=resumed_from_run_id,
                    resumed_from_stage=resumed_from_stage,
                    available_checkpoint_run_id=run_id,
                    available_checkpoint_stage=stage_name,
                ),
                phase_metrics=phase_metrics,
                total_duration_seconds=checkpoint_duration_baseline + seconds_since(started),
                stream_summary=stream_summary,
            )
            state_store.persist_run_checkpoint(
                run_id=run_id,
                run_key=run_key,
                attempt_number=attempt_number,
                stage_name=stage_name,
                checkpointed_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                total_duration_seconds=checkpoint_duration_baseline + seconds_since(started),
                phase_metrics=_deep_copy_phase_metrics(phase_metrics),
                normalized_rows=normalized_rows,
                match_rows=match_rows,
                blocking_metrics_rows=blocking_metrics_rows,
                cluster_rows=clustered_rows,
                golden_rows=golden_rows,
                crosswalk_rows=crosswalk_rows,
                review_rows=review_rows,
                active_review_rows=active_review_rows,
                summary=checkpoint_summary,
            )
            latest_checkpoint_run_id = run_id
            latest_checkpoint_stage = stage_name

        if not stream_events_file.exists():
            _persist_stream_event_snapshot(batch=resolved_batch, output_path=stream_events_file)

        if resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["normalize"]:
            normalize_started = time.perf_counter()
            _persist_stream_event_snapshot(batch=resolved_batch, output_path=stream_events_file)
            normalized_rows, applied_stream_summary = apply_stream_event_batch(
                previous_rows=source_bundle.normalized_rows,
                batch=resolved_batch,
                config=config,
            )
            stream_summary.update(applied_stream_summary)
            stream_summary["source_run_id"] = source_run_id
            stream_summary["event_snapshot_path"] = str(stream_events_file)
            run_context["person_count"] = len(
                {
                    str(row.get("person_entity_id", "")).strip()
                    for row in normalized_rows
                    if str(row.get("person_entity_id", "")).strip()
                }
            )
            write_csv_dicts(normalized_file, normalized_rows, fieldnames=NORMALIZED_HEADERS)
            print(f"stream-refreshed normalized rows written: {normalized_file}")
            normalize_metric = _build_stream_phase_metric(
                seconds_since(normalize_started),
                event_count=len(resolved_batch.events),
                output_record_count=len(normalized_rows),
            )
            phase_metrics["normalize"] = normalize_metric
            phase_metrics["stream_apply_events"] = dict(normalize_metric)
            persist_checkpoint("normalize")

        if resume_stage_order < RUN_CHECKPOINT_STAGE_ORDER["match"]:
            incremental_result = refresh_incremental_run(
                current_rows=normalized_rows,
                previous_bundle=source_bundle,
                config=config,
            )
            match_rows = incremental_result.match_rows
            blocking_metrics_rows = incremental_result.blocking_metrics_rows
            clustered_rows = incremental_result.clustered_rows
            golden_rows = incremental_result.golden_rows
            crosswalk_rows = incremental_result.crosswalk_rows
            active_review_rows = incremental_result.active_review_rows
            review_rows = incremental_result.review_rows
            refresh_summary = {
                **incremental_result.metadata,
                "mode": "incremental",
                "fallback_to_full": False,
                "predecessor_run_id": source_run_id,
            }

            match_started = time.perf_counter()
            _write_precomputed_match_outputs(matches_file, match_rows, blocking_metrics_rows)
            phase_metrics["match"] = _build_phase_metric(
                seconds_since(match_started),
                input_record_count=len(normalized_rows),
                output_record_count=len(match_rows),
                candidate_pair_count=len(match_rows),
            )
            persist_checkpoint("match")

            cluster_started = time.perf_counter()
            _write_precomputed_cluster_output(
                clusters_file,
                _build_cluster_output_rows(clustered_rows),
            )
            phase_metrics["cluster"] = _build_phase_metric(
                seconds_since(cluster_started),
                input_record_count=len(normalized_rows),
                output_record_count=len(clustered_rows),
            )
            persist_checkpoint("cluster")

            review_queue_started = time.perf_counter()
            _write_precomputed_review_queue_output(review_queue_file, review_rows)
            phase_metrics["review_queue"] = _build_phase_metric(
                seconds_since(review_queue_started),
                input_record_count=len(match_rows),
                output_record_count=len(active_review_rows),
            )
            persist_checkpoint("review_queue")

            golden_started = time.perf_counter()
            _write_precomputed_golden_output(golden_file, golden_rows)
            phase_metrics["golden"] = _build_phase_metric(
                seconds_since(golden_started),
                input_record_count=len(clustered_rows),
                output_record_count=len(golden_rows),
            )
            persist_checkpoint("golden")

            crosswalk_started = time.perf_counter()
            _write_precomputed_crosswalk_output(crosswalk_file, crosswalk_rows)
            phase_metrics["crosswalk"] = _build_phase_metric(
                seconds_since(crosswalk_started),
                input_record_count=len(clustered_rows),
                output_record_count=len(crosswalk_rows),
            )
            persist_checkpoint("crosswalk")

        resume_summary = _build_resume_summary(
            resumed_from_run_id=resumed_from_run_id,
            resumed_from_stage=resumed_from_stage,
            available_checkpoint_run_id=latest_checkpoint_run_id,
            available_checkpoint_stage=latest_checkpoint_stage,
        )
        report_started = time.perf_counter()
        summary = _write_quality_outputs(
            report_file,
            normalized_file,
            normalized_rows,
            candidate_pair_count=len(match_rows),
            decision_counts=_decision_counts(match_rows),
            cluster_count=_cluster_count(clustered_rows),
            golden_record_count=len(golden_rows),
            review_queue_count=len(active_review_rows),
            summary_updates={
                "run_context": run_context,
                "refresh": refresh_summary,
                "resume": resume_summary,
                "stream": stream_summary,
                "performance": {
                    "total_duration_seconds": round(
                        checkpoint_duration_baseline + seconds_since(started),
                        6,
                    ),
                    "phase_metrics": _deep_copy_phase_metrics(phase_metrics),
                },
            },
        )
        phase_metrics["report"] = _build_phase_metric(
            seconds_since(report_started),
            input_record_count=len(normalized_rows),
            output_record_count=1,
        )
        summary["performance"] = {
            "total_duration_seconds": round(checkpoint_duration_baseline + seconds_since(started), 6),
            "phase_metrics": _deep_copy_phase_metrics(phase_metrics),
        }
        _write_run_summary_artifacts(
            report_file=report_file,
            normalized_file=normalized_file,
            summary=summary,
        )
        persist_checkpoint("report")

        persist_started = time.perf_counter()
        state_store.persist_run(
            metadata=PersistRunMetadata(
                run_id=run_id,
                run_key=run_key,
                attempt_number=attempt_number,
                batch_id=resolved_batch.batch_id,
                input_mode="event_stream",
                manifest_path=str(resolved_batch.event_path),
                base_dir=str(base.resolve()),
                config_dir=str(Path(args.config_dir).resolve()) if args.config_dir else None,
                profile=None,
                seed=None,
                formats="jsonl",
                started_at_utc=run_started,
                finished_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                status="completed",
            ),
            normalized_rows=normalized_rows,
            match_rows=match_rows,
            blocking_metrics_rows=blocking_metrics_rows,
            cluster_rows=read_dict_rows(clusters_file),
            golden_rows=golden_rows,
            crosswalk_rows=crosswalk_rows,
            review_rows=review_rows,
            summary=summary,
        )
        phase_metrics["persist_state"] = _build_phase_metric(
            seconds_since(persist_started),
            input_record_count=len(normalized_rows),
            output_record_count=1,
        )
        summary["performance"] = {
            "total_duration_seconds": round(checkpoint_duration_baseline + seconds_since(started), 6),
            "phase_metrics": _deep_copy_phase_metrics(phase_metrics),
        }
        state_store.update_run_summary(run_id=run_id, summary=summary)
        _write_run_summary_artifacts(
            report_file=report_file,
            normalized_file=normalized_file,
            summary=summary,
        )
        _record_cli_audit_event(
            state_store,
            action="stream_refresh",
            resource_type="pipeline_run",
            resource_id=run_id,
            run_id=run_id,
            status="succeeded",
            details={
                "source_run_id": source_run_id,
                "stream_id": resolved_batch.stream_id,
                "batch_id": resolved_batch.batch_id,
                "event_count": len(resolved_batch.events),
                "event_sha256": resolved_batch.content_sha256,
            },
        )
        emit_structured_log(
            "stream_refresh_completed",
            component="cli",
            command="stream-refresh",
            run_id=run_id,
            source_run_id=source_run_id,
            stream_id=resolved_batch.stream_id,
            event_count=len(resolved_batch.events),
            total_records=summary["total_records"],
            candidate_pair_count=summary["candidate_pair_count"],
            cluster_count=summary["cluster_count"],
            golden_record_count=summary["golden_record_count"],
            review_queue_count=summary["review_queue_count"],
            duration_seconds=summary["performance"]["total_duration_seconds"],
        )
        print(json.dumps({"action": "stream_refreshed", "run_id": run_id, "summary": summary}, sort_keys=True))
    except Exception as exc:
        failure_summary = None
        if normalized_rows:
            failure_summary = _build_pipeline_summary(
                normalized_rows=normalized_rows,
                match_rows=match_rows,
                clustered_rows=clustered_rows,
                golden_rows=golden_rows,
                active_review_rows=active_review_rows,
                run_context=run_context,
                refresh_summary=refresh_summary,
                resume_summary=_build_resume_summary(
                    resumed_from_run_id=resumed_from_run_id,
                    resumed_from_stage=resumed_from_stage,
                    available_checkpoint_run_id=latest_checkpoint_run_id,
                    available_checkpoint_stage=latest_checkpoint_stage,
                ),
                phase_metrics=phase_metrics,
                total_duration_seconds=checkpoint_duration_baseline + seconds_since(started),
                stream_summary=stream_summary,
            )
        if run_id is not None:
            state_store.mark_run_failed(
                run_id=run_id,
                finished_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                failure_detail=str(exc),
                summary=failure_summary,
            )
            _record_cli_audit_event(
                state_store,
                action="stream_refresh",
                resource_type="pipeline_run",
                resource_id=run_id,
                run_id=run_id,
                status="failed",
                details={
                    "source_run_id": source_run_id,
                    "stream_id": resolved_batch.stream_id,
                    "batch_id": resolved_batch.batch_id,
                    "error": str(exc),
                },
            )
        emit_structured_log(
            "stream_refresh_failed",
            component="cli",
            command="stream-refresh",
            run_id=run_id or "",
            source_run_id=source_run_id,
            stream_id=resolved_batch.stream_id,
            duration_seconds=seconds_since(started),
            error=str(exc),
            level="ERROR",
        )
        raise


def _cmd_benchmark_run(args: argparse.Namespace) -> None:
    benchmark_config_dir = Path(args.config_dir) if args.config_dir else None
    fixtures = load_benchmark_fixture_configs(benchmark_config_dir, environment=args.environment)
    fixture = fixtures.get(args.fixture)
    if fixture is None:
        raise ValueError(
            f"Unknown benchmark fixture {args.fixture!r}; expected one of {sorted(fixtures)}"
        )

    deployment_name = args.deployment_target
    if args.enforce_targets and deployment_name not in fixture.capacity_targets:
        raise ValueError(
            f"Benchmark fixture {fixture.name!r} does not define capacity targets for "
            f"deployment {deployment_name!r}"
        )
    target = fixture.capacity_targets.get(deployment_name)
    if (
        args.environment
        and target is not None
        and target.runtime_environment is not None
        and args.environment != target.runtime_environment
    ):
        raise ValueError(
            f"Benchmark fixture {fixture.name!r} deployment target {deployment_name!r} requires "
            f"--environment {target.runtime_environment!r}, received {args.environment!r}"
        )
    runtime_environment = args.environment or (None if target is None else target.runtime_environment)

    benchmark_root = Path(args.output_dir) / fixture.name
    if benchmark_root.exists():
        shutil.rmtree(benchmark_root)
    benchmark_root.mkdir(parents=True, exist_ok=True)

    benchmark_summary_path = benchmark_root / "benchmark_summary.json"
    benchmark_report_path = benchmark_root / "benchmark_report.md"

    emit_structured_log(
        "benchmark_run_requested",
        component="cli",
        command="benchmark-run",
        fixture=fixture.name,
        deployment_target=deployment_name,
        output_dir=benchmark_root,
        runtime_environment=runtime_environment or "",
        state_store_backend="" if target is None else target.state_store_backend,
    )

    def apply_runtime_overrides(argv: list[str]) -> list[str]:
        if runtime_environment:
            argv.extend(["--environment", runtime_environment])
        if args.runtime_config:
            argv.extend(["--runtime-config", args.runtime_config])
        if args.config_dir:
            argv.extend(["--config-dir", args.config_dir])
        return argv

    with benchmark_execution_context(
        benchmark_root=benchmark_root,
        target=target,
        explicit_state_db=args.state_db,
        runtime_environment=runtime_environment,
    ) as execution_target:
        emit_structured_log(
            "benchmark_runtime_prepared",
            component="cli",
            command="benchmark-run",
            fixture=fixture.name,
            deployment_target=deployment_name,
            runtime_environment=execution_target.runtime_environment or "",
            state_store_backend=execution_target.state_store_backend,
            state_db=execution_target.state_db_display_name,
            state_store_mode=execution_target.state_store_mode,
        )

        state_db = execution_target.state_db
        run_artifact_root: Path
        run_summary_path: Path
        run_summary: dict[str, object]
        continuous_ingest_summary: dict[str, object] | None = None

        if fixture.mode == "event_stream":
            seed_artifact_root = benchmark_root / "seed_run"
            main(
                apply_runtime_overrides(
                    [
                        "run-all",
                        "--base-dir",
                        str(seed_artifact_root),
                        "--profile",
                        fixture.profile,
                        "--seed",
                        str(fixture.seed),
                        "--duplicate-rate",
                        str(fixture.duplicate_rate),
                        "--formats",
                        ",".join(fixture.formats),
                        "--person-count",
                        str(fixture.person_count),
                        "--state-db",
                        state_db,
                    ]
                )
            )

            store = PipelineStateStore(state_db)
            current_source_run_id = store.latest_completed_run_id()
            if current_source_run_id is None:
                raise RuntimeError("benchmark seed run did not persist a completed run")
            seed_run_id = current_source_run_id

            total_event_count = 0
            total_stream_duration_seconds = 0.0
            stream_run_ids: list[str] = []
            event_batch_paths: list[str] = []
            batch_durations_seconds: list[float] = []
            run_artifact_root = seed_artifact_root
            run_summary_path = seed_artifact_root / "data" / "exceptions" / "run_summary.json"
            run_summary = json.loads(run_summary_path.read_text(encoding="utf-8"))

            for batch_index in range(1, fixture.stream_batch_count + 1):
                source_bundle = store.load_run_bundle(current_source_run_id)
                events = synthesize_stream_events(
                    source_bundle.normalized_rows,
                    stream_id=f"{fixture.name}_stream",
                    batch_index=batch_index,
                    events_per_batch=fixture.stream_events_per_batch,
                )
                event_file = benchmark_root / "events" / f"batch_{batch_index:03d}.jsonl"
                write_stream_events_jsonl(event_file, events)
                event_batch_paths.append(str(event_file))

                stream_artifact_root = benchmark_root / "stream_runs" / f"batch_{batch_index:03d}"
                main(
                    apply_runtime_overrides(
                        [
                            "stream-refresh",
                            "--base-dir",
                            str(stream_artifact_root),
                            "--state-db",
                            state_db,
                            "--source-run-id",
                            current_source_run_id,
                            "--events",
                            str(event_file),
                            "--stream-id",
                            f"{fixture.name}_stream",
                        ]
                    )
                )

                current_source_run_id = store.latest_completed_run_id()
                if current_source_run_id is None:
                    raise RuntimeError("benchmark stream refresh did not persist a completed run")
                stream_run_ids.append(current_source_run_id)
                run_artifact_root = stream_artifact_root
                run_summary_path = stream_artifact_root / "data" / "exceptions" / "run_summary.json"
                run_summary = json.loads(run_summary_path.read_text(encoding="utf-8"))
                total_event_count += len(events)
                performance = run_summary.get("performance", {})
                batch_duration_seconds = 0.0
                if isinstance(performance, dict):
                    batch_duration_seconds = float(
                        performance.get("total_duration_seconds", 0.0) or 0.0
                    )
                total_stream_duration_seconds += batch_duration_seconds
                batch_durations_seconds.append(round(batch_duration_seconds, 6))

            p95_batch_duration_seconds = 0.0
            if batch_durations_seconds:
                ordered_batch_durations = sorted(batch_durations_seconds)
                p95_index = max(0, ((95 * len(ordered_batch_durations) + 99) // 100) - 1)
                p95_batch_duration_seconds = ordered_batch_durations[p95_index]

            continuous_ingest_summary = {
                "mode": "event_stream",
                "stream_id": f"{fixture.name}_stream",
                "batch_count": fixture.stream_batch_count,
                "events_per_batch": fixture.stream_events_per_batch,
                "total_event_count": total_event_count,
                "total_stream_duration_seconds": round(total_stream_duration_seconds, 6),
                "events_per_second": _rate(total_event_count, total_stream_duration_seconds),
                "max_batch_duration_seconds": max(batch_durations_seconds, default=0.0),
                "p95_batch_duration_seconds": round(p95_batch_duration_seconds, 6),
                "batch_durations_seconds": batch_durations_seconds,
                "seed_run_id": seed_run_id,
                "final_run_id": current_source_run_id,
                "stream_run_ids": stream_run_ids,
                "event_batch_paths": event_batch_paths,
                "last_stream_run_summary_path": str(run_summary_path),
            }
        else:
            run_artifact_root = benchmark_root / "run_artifacts"
            main(
                apply_runtime_overrides(
                    [
                        "run-all",
                        "--base-dir",
                        str(run_artifact_root),
                        "--profile",
                        fixture.profile,
                        "--seed",
                        str(fixture.seed),
                        "--duplicate-rate",
                        str(fixture.duplicate_rate),
                        "--formats",
                        ",".join(fixture.formats),
                        "--person-count",
                        str(fixture.person_count),
                        "--state-db",
                        state_db,
                    ]
                )
            )
            run_summary_path = run_artifact_root / "data" / "exceptions" / "run_summary.json"
            run_summary = json.loads(run_summary_path.read_text(encoding="utf-8"))

        benchmark_summary = build_benchmark_summary(
            fixture=fixture,
            deployment_name=deployment_name,
            run_summary=run_summary,
            benchmark_root=benchmark_root,
            run_artifact_root=run_artifact_root,
            run_summary_path=run_summary_path,
            runtime_environment=execution_target.runtime_environment,
            state_store_backend=execution_target.state_store_backend,
            state_db_display_name=execution_target.state_db_display_name,
            state_store_mode=execution_target.state_store_mode,
            continuous_ingest=continuous_ingest_summary,
        )
    benchmark_summary_path.write_text(
        json.dumps(benchmark_summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_markdown(benchmark_report_path, build_benchmark_report_markdown(benchmark_summary))

    capacity_assertions = benchmark_summary["capacity_assertions"]
    emit_structured_log(
        "benchmark_run_completed",
        component="cli",
        command="benchmark-run",
        fixture=fixture.name,
        deployment_target=deployment_name,
        status=capacity_assertions["status"],
        benchmark_summary_path=benchmark_summary_path,
    )

    print(f"benchmark summary written: {benchmark_summary_path}")
    print(f"benchmark report written: {benchmark_report_path}")
    print(f"capacity assertion status: {capacity_assertions['status']}")

    if args.enforce_targets and capacity_assertions["status"] != "passed":
        raise RuntimeError(
            f"Benchmark fixture {fixture.name!r} missed one or more capacity targets "
            f"for deployment {deployment_name!r}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl-identity-engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="Generate synthetic source records.")
    generate_parser.add_argument("--output", default="data/synthetic_sources")
    generate_parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    generate_parser.add_argument("--seed", default=42, type=int)
    generate_parser.add_argument(
        "--person-count",
        default=None,
        type=int,
        help="Override the synthetic person-entity count for scale testing or benchmark fixtures.",
    )
    generate_parser.add_argument("--duplicate-rate", default=None, type=float)
    generate_parser.add_argument("--formats", default="csv,parquet")
    generate_parser.set_defaults(func=_cmd_generate)

    normalize_parser = subparsers.add_parser("normalize", help="Normalize source records.")
    normalize_parser.add_argument(
        "--input",
        action="append",
        default=[],
        help=(
            "Path to a source CSV or Parquet file. Repeat to normalize multiple explicit inputs."
        ),
    )
    normalize_parser.add_argument(
        "--input-dir",
        default=DEFAULT_NORMALIZE_INPUT_DIR,
        help=(
            "Directory to scan for person_source_*.csv files, or person_source_*.parquet "
            "when CSV files are absent, if --input is not provided."
        ),
    )
    normalize_parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "Path to a production batch manifest (.json, .yaml, or .yml). "
            "When provided, manifest inputs are validated before normalization starts."
        ),
    )
    normalize_parser.add_argument(
        "--output",
        default="data/normalized/normalized_person_records.csv",
    )
    normalize_parser.add_argument("--environment", default=None)
    normalize_parser.add_argument("--runtime-config", default=None)
    normalize_parser.add_argument("--config-dir", default=None)
    normalize_parser.set_defaults(func=_cmd_normalize)

    match_parser = subparsers.add_parser("match", help="Generate and score candidate pairs.")
    match_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    match_parser.add_argument("--output", default="data/matches/candidate_scores.csv")
    match_parser.add_argument("--environment", default=None)
    match_parser.add_argument("--runtime-config", default=None)
    match_parser.add_argument("--config-dir", default=None)
    match_parser.set_defaults(func=_cmd_match)

    cluster_parser = subparsers.add_parser("cluster", help="Build entity clusters from accepted links.")
    cluster_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    cluster_parser.add_argument("--output", default="data/matches/entity_clusters.csv")
    cluster_parser.add_argument(
        "--matches",
        default=None,
        help="Path to candidate_scores.csv. Defaults relative to --input.",
    )
    cluster_parser.set_defaults(func=_cmd_cluster)

    review_queue_parser = subparsers.add_parser(
        "review-queue",
        help="Build the manual review queue from candidate scores.",
    )
    review_queue_parser.add_argument("--input", default="data/matches/candidate_scores.csv")
    review_queue_parser.add_argument(
        "--output",
        default="data/review_queue/manual_review_queue.csv",
    )
    review_queue_parser.set_defaults(func=_cmd_review_queue)

    golden_parser = subparsers.add_parser("golden", help="Build golden records.")
    golden_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    golden_parser.add_argument("--output", default="data/golden/golden_person_records.csv")
    golden_parser.add_argument(
        "--clusters",
        default=None,
        help=(
            "Path to entity_clusters.csv when the input rows do not already include cluster_id. "
            "Defaults to the matching artifact derived from --input."
        ),
    )
    golden_parser.add_argument("--environment", default=None)
    golden_parser.add_argument("--runtime-config", default=None)
    golden_parser.add_argument("--config-dir", default=None)
    golden_parser.set_defaults(func=_cmd_golden)

    public_safety_demo_parser = subparsers.add_parser(
        "public-safety-demo",
        help="Build the synthetic CAD/RMS incident-to-identity demo views.",
    )
    public_safety_demo_parser.add_argument("--base-dir", default=".")
    public_safety_demo_parser.add_argument(
        "--incident-records",
        default=None,
        help="Path to incident_records.csv or .parquet. Defaults under data/synthetic_sources/.",
    )
    public_safety_demo_parser.add_argument(
        "--incident-links",
        default=None,
        help="Path to incident_person_links.csv or .parquet. Defaults under data/synthetic_sources/.",
    )
    public_safety_demo_parser.add_argument(
        "--golden-file",
        default=None,
        help="Path to golden_person_records.csv. Defaults under data/golden/.",
    )
    public_safety_demo_parser.add_argument(
        "--crosswalk-file",
        default=None,
        help="Path to source_to_golden_crosswalk.csv. Defaults under data/golden/.",
    )
    public_safety_demo_parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for the public-safety demo artifacts. Defaults under data/public_safety_demo/.",
    )
    public_safety_demo_parser.set_defaults(func=_cmd_public_safety_demo)

    report_parser = subparsers.add_parser("report", help="Produce run report.")
    report_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    report_parser.add_argument("--output", default="data/exceptions/run_report.md")
    report_parser.add_argument("--environment", default=None)
    report_parser.add_argument("--runtime-config", default=None)
    report_parser.add_argument(
        "--state-db",
        default=None,
        help="Path or SQLAlchemy URL for a persisted state store when reloading a completed run.",
    )
    report_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to reload from --state-db.",
    )
    report_parser.add_argument(
        "--matches",
        default=None,
        help="Path to candidate_scores.csv. Defaults relative to --input.",
    )
    report_parser.add_argument(
        "--clusters",
        default=None,
        help="Path to entity_clusters.csv. Defaults relative to --input.",
    )
    report_parser.add_argument(
        "--golden-file",
        default=None,
        help="Path to golden_person_records.csv. Defaults relative to --input.",
    )
    report_parser.add_argument(
        "--review-queue",
        default=None,
        help="Path to manual_review_queue.csv. Defaults relative to --input.",
    )
    report_parser.set_defaults(func=_cmd_report)

    state_db_upgrade_parser = subparsers.add_parser(
        "state-db-upgrade",
        help="Upgrade a persisted state store to the latest Alembic revision.",
    )
    state_db_upgrade_parser.add_argument("--environment", default=None)
    state_db_upgrade_parser.add_argument("--runtime-config", default=None)
    state_db_upgrade_parser.add_argument("--state-db", default=None)
    state_db_upgrade_parser.add_argument("--revision", default="head")
    state_db_upgrade_parser.set_defaults(func=_cmd_state_db_upgrade)

    state_db_current_parser = subparsers.add_parser(
        "state-db-current",
        help="Show the current Alembic revision for a persisted state store.",
    )
    state_db_current_parser.add_argument("--environment", default=None)
    state_db_current_parser.add_argument("--runtime-config", default=None)
    state_db_current_parser.add_argument("--state-db", default=None)
    state_db_current_parser.set_defaults(func=_cmd_state_db_current)

    review_case_list_parser = subparsers.add_parser(
        "review-case-list",
        help="List persisted manual-review cases for a completed run.",
    )
    review_case_list_parser.add_argument("--environment", default=None)
    review_case_list_parser.add_argument("--runtime-config", default=None)
    review_case_list_parser.add_argument("--state-db", default=None)
    review_case_list_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to inspect. Defaults to the latest completed run with review cases.",
    )
    review_case_list_parser.add_argument(
        "--status",
        default=None,
        choices=REVIEW_CASE_STATUSES,
        help="Optional lifecycle status filter.",
    )
    review_case_list_parser.add_argument(
        "--assigned-to",
        default=None,
        help="Optional assignee filter.",
    )
    review_case_list_parser.set_defaults(func=_cmd_review_case_list)

    review_case_update_parser = subparsers.add_parser(
        "review-case-update",
        help="Update persisted manual-review case status, assignee, or notes.",
    )
    review_case_update_parser.add_argument("--environment", default=None)
    review_case_update_parser.add_argument("--runtime-config", default=None)
    review_case_update_parser.add_argument("--state-db", default=None)
    review_case_update_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to update. Defaults to the latest completed run with review cases.",
    )
    review_case_update_parser.add_argument("--review-id", required=True)
    review_case_update_parser.add_argument(
        "--status",
        default=None,
        choices=REVIEW_CASE_STATUSES,
        help="Optional lifecycle status transition.",
    )
    review_case_update_parser.add_argument(
        "--assigned-to",
        default=None,
        help="Optional assignee value. Use an empty string to clear it.",
    )
    review_case_update_parser.add_argument(
        "--notes",
        default=None,
        help="Optional operator notes value. Use an empty string to clear it.",
    )
    review_case_update_parser.set_defaults(func=_cmd_review_case_update)

    apply_review_decision_parser = subparsers.add_parser(
        "apply-review-decision",
        help="Apply an operator decision to a persisted review case with idempotent output.",
    )
    apply_review_decision_parser.add_argument("--environment", default=None)
    apply_review_decision_parser.add_argument("--runtime-config", default=None)
    apply_review_decision_parser.add_argument("--state-db", default=None)
    apply_review_decision_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to update. Defaults to the latest completed run with review cases.",
    )
    apply_review_decision_parser.add_argument("--review-id", required=True)
    apply_review_decision_parser.add_argument(
        "--decision",
        required=True,
        choices=REVIEW_CASE_STATUSES,
        help="Target lifecycle status for the review case.",
    )
    apply_review_decision_parser.add_argument(
        "--assigned-to",
        default=None,
        help="Optional assignee value. Use an empty string to clear it.",
    )
    apply_review_decision_parser.add_argument(
        "--notes",
        default=None,
        help="Optional operator notes value. Use an empty string to clear it.",
    )
    apply_review_decision_parser.set_defaults(func=_cmd_apply_review_decision)

    publish_delivery_parser = subparsers.add_parser(
        "publish-delivery",
        help="Publish versioned downstream golden and crosswalk snapshots from persisted state.",
    )
    publish_delivery_parser.add_argument("--environment", default=None)
    publish_delivery_parser.add_argument("--runtime-config", default=None)
    publish_delivery_parser.add_argument("--state-db", default=None)
    publish_delivery_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to publish. Defaults to the latest completed run in --state-db.",
    )
    publish_delivery_parser.add_argument(
        "--output-dir",
        default="published/delivery",
        help="Root directory where versioned delivery snapshots should be published.",
    )
    publish_delivery_parser.add_argument(
        "--contract-version",
        default=DELIVERY_CONTRACT_VERSION,
        help="Versioned consumer contract to publish. Defaults to the current stable delivery contract.",
    )
    publish_delivery_parser.set_defaults(func=_cmd_publish_delivery)

    publish_run_parser = subparsers.add_parser(
        "publish-run",
        help="Trigger downstream publication for a persisted run with JSON operator output.",
    )
    publish_run_parser.add_argument("--environment", default=None)
    publish_run_parser.add_argument("--runtime-config", default=None)
    publish_run_parser.add_argument("--state-db", default=None)
    publish_run_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to publish. Defaults to the latest completed run in --state-db.",
    )
    publish_run_parser.add_argument(
        "--output-dir",
        default="published/delivery",
        help="Root directory where versioned delivery snapshots should be published.",
    )
    publish_run_parser.add_argument(
        "--contract-version",
        default=DELIVERY_CONTRACT_VERSION,
        help="Versioned consumer contract to publish. Defaults to the current stable delivery contract.",
    )
    publish_run_parser.set_defaults(func=_cmd_publish_run)

    export_job_list_parser = subparsers.add_parser(
        "export-job-list",
        help="List configured downstream export jobs.",
    )
    export_job_list_parser.add_argument("--environment", default=None)
    export_job_list_parser.add_argument("--runtime-config", default=None)
    export_job_list_parser.add_argument("--config-dir", default=None)
    export_job_list_parser.set_defaults(func=_cmd_export_job_list)

    export_job_run_parser = subparsers.add_parser(
        "export-job-run",
        help="Run a configured downstream export job and persist an audit record.",
    )
    export_job_run_parser.add_argument("--environment", default=None)
    export_job_run_parser.add_argument("--runtime-config", default=None)
    export_job_run_parser.add_argument("--config-dir", default=None)
    export_job_run_parser.add_argument("--state-db", default=None)
    export_job_run_parser.add_argument("--job-name", required=True)
    export_job_run_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to export. Defaults to the latest completed run in --state-db.",
    )
    export_job_run_parser.set_defaults(func=_cmd_export_job_run)

    export_job_history_parser = subparsers.add_parser(
        "export-job-history",
        help="List tracked downstream export-job runs from persisted state.",
    )
    export_job_history_parser.add_argument("--environment", default=None)
    export_job_history_parser.add_argument("--runtime-config", default=None)
    export_job_history_parser.add_argument("--state-db", default=None)
    export_job_history_parser.add_argument("--job-name", default=None)
    export_job_history_parser.add_argument("--source-run-id", default=None)
    export_job_history_parser.add_argument(
        "--status",
        default=None,
        choices=sorted(EXPORT_RUN_STATUSES),
        help="Optional export-run status filter.",
    )
    export_job_history_parser.set_defaults(func=_cmd_export_job_history)

    verify_replay_bundle_parser = subparsers.add_parser(
        "verify-replay-bundle",
        help="Verify the archived replay bundle for a persisted manifest-backed run.",
    )
    verify_replay_bundle_parser.add_argument("--environment", default=None)
    verify_replay_bundle_parser.add_argument("--runtime-config", default=None)
    verify_replay_bundle_parser.add_argument("--state-db", default=None)
    verify_replay_bundle_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to verify. Defaults to the latest completed run in --state-db.",
    )
    verify_replay_bundle_parser.set_defaults(func=_cmd_verify_replay_bundle)

    replay_run_parser = subparsers.add_parser(
        "replay-run",
        help="Replay a persisted manifest-backed run through run-all.",
    )
    replay_run_parser.add_argument("--environment", default=None)
    replay_run_parser.add_argument("--runtime-config", default=None)
    replay_run_parser.add_argument("--state-db", default=None)
    replay_run_parser.add_argument(
        "--run-id",
        default=None,
        help="Persisted run ID to replay. Defaults to the latest completed run in --state-db.",
    )
    replay_run_parser.add_argument(
        "--base-dir",
        default=None,
        help="Optional output base directory override. Defaults to the stored base_dir for the source run.",
    )
    replay_run_parser.add_argument(
        "--refresh-mode",
        default=None,
        choices=["full", "incremental"],
        help="Optional replay refresh mode override. Defaults to the stored run refresh mode.",
    )
    replay_run_parser.set_defaults(func=_cmd_replay_run)

    serve_api_parser = subparsers.add_parser(
        "serve-api",
        help="Serve the operator API over a persisted SQL state store.",
    )
    serve_api_parser.add_argument("--environment", default=None)
    serve_api_parser.add_argument("--runtime-config", default=None)
    serve_api_parser.add_argument("--state-db", default=None)
    serve_api_parser.add_argument("--host", default="127.0.0.1")
    serve_api_parser.add_argument("--port", default=8000, type=int)
    serve_api_parser.add_argument("--log-level", default="info")
    serve_api_parser.set_defaults(func=_cmd_serve_api)

    stream_refresh_parser = subparsers.add_parser(
        "stream-refresh",
        help="Apply an ordered event batch onto a persisted run and recompute affected entities.",
    )
    stream_refresh_parser.add_argument("--environment", default=None)
    stream_refresh_parser.add_argument("--runtime-config", default=None)
    stream_refresh_parser.add_argument("--config-dir", default=None)
    stream_refresh_parser.add_argument("--state-db", default=None)
    stream_refresh_parser.add_argument("--base-dir", default=".")
    stream_refresh_parser.add_argument(
        "--source-run-id",
        default=None,
        help="Completed persisted run to refresh. Defaults to the latest completed run in --state-db.",
    )
    stream_refresh_parser.add_argument(
        "--events",
        required=True,
        help="Path to the ordered event batch in JSONL/NDJSON format.",
    )
    stream_refresh_parser.add_argument(
        "--stream-id",
        default=None,
        help="Optional default stream identifier when the event lines omit stream_id.",
    )
    stream_refresh_parser.set_defaults(func=_cmd_stream_refresh)

    benchmark_run_parser = subparsers.add_parser(
        "benchmark-run",
        help="Run a named large-batch benchmark fixture and evaluate its capacity targets.",
    )
    benchmark_run_parser.add_argument("--fixture", required=True)
    benchmark_run_parser.add_argument(
        "--deployment-target",
        default="single_host_container",
        help="Capacity-target deployment profile to evaluate for the selected benchmark fixture.",
    )
    benchmark_run_parser.add_argument(
        "--output-dir",
        default="dist/benchmarks",
        help="Directory where benchmark artifacts and the nested pipeline run should be written.",
    )
    benchmark_run_parser.add_argument(
        "--state-db",
        default=None,
        help="Optional persisted state-store path or SQLAlchemy URL override for the benchmark run.",
    )
    benchmark_run_parser.add_argument("--environment", default=None)
    benchmark_run_parser.add_argument("--runtime-config", default=None)
    benchmark_run_parser.add_argument("--config-dir", default=None)
    benchmark_run_parser.add_argument(
        "--enforce-targets",
        dest="enforce_targets",
        action="store_true",
        help="Fail the command if the selected benchmark fixture misses a configured capacity target.",
    )
    benchmark_run_parser.add_argument(
        "--no-enforce-targets",
        dest="enforce_targets",
        action="store_false",
        help="Write benchmark artifacts without failing the command on target misses.",
    )
    benchmark_run_parser.set_defaults(func=_cmd_benchmark_run, enforce_targets=True)

    run_all_parser = subparsers.add_parser("run-all", help="Run all scaffold stages in sequence.")
    run_all_parser.add_argument("--base-dir", default=".")
    run_all_parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    run_all_parser.add_argument("--seed", default=42, type=int)
    run_all_parser.add_argument(
        "--person-count",
        default=None,
        type=int,
        help="Override the synthetic person-entity count for scale testing or benchmark fixtures.",
    )
    run_all_parser.add_argument("--duplicate-rate", default=None, type=float)
    run_all_parser.add_argument("--formats", default="csv,parquet")
    run_all_parser.add_argument("--refresh-mode", default="full", choices=["full", "incremental"])
    run_all_parser.add_argument("--environment", default=None)
    run_all_parser.add_argument("--runtime-config", default=None)
    run_all_parser.add_argument("--replay-source-run-id", default=None, help=argparse.SUPPRESS)
    run_all_parser.add_argument(
        "--state-db",
        default=None,
        help="Path or SQLAlchemy URL for the persisted state store where completed runs should be recorded.",
    )
    run_all_parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "Path to a production batch manifest (.json, .yaml, or .yml). "
            "When provided, run-all skips synthetic generation and uses the validated manifest inputs."
        ),
    )
    run_all_parser.add_argument("--config-dir", default=None)
    run_all_parser.set_defaults(func=_cmd_run_all)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _apply_runtime_defaults(args)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
