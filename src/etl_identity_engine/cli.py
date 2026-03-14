"""Command-line entrypoint for the ETL Identity Engine scaffold."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from collections import Counter
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Sequence

from etl_identity_engine.delivery_publish import publish_delivery_snapshot
from etl_identity_engine.generate.synth_generator import generate_synthetic_sources
from etl_identity_engine.incremental_refresh import refresh_incremental_run
from etl_identity_engine.ingest.manifest import (
    ResolvedBatchManifest,
    peek_manifest_batch_id,
    resolve_batch_manifest,
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
from etl_identity_engine.output_contracts import (
    BLOCKING_METRICS_HEADERS,
    CROSSWALK_HEADERS,
    DELIVERY_CONTRACT_VERSION,
    ENTITY_CLUSTER_HEADERS,
    EXCEPTION_HEADERS,
    GOLDEN_HEADERS,
    MANUAL_REVIEW_HEADERS,
    MATCH_SCORE_HEADERS,
    NORMALIZED_HEADERS,
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
    PipelineConfig,
    load_pipeline_config,
    load_runtime_environment,
)
from etl_identity_engine.storage.migration_runner import (
    current_sqlite_store_revision,
    head_revision,
    upgrade_sqlite_store,
)
from etl_identity_engine.storage.sqlite_store import (
    PersistedReviewCase,
    PersistRunMetadata,
    SQLitePipelineStore,
    build_run_key,
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
        "publish-delivery",
        "run-all",
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


def _require_state_db(args: argparse.Namespace) -> Path:
    if not args.state_db:
        raise ValueError("This command requires --state-db or a runtime environment with state_db configured")
    return Path(args.state_db)


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

    store = SQLitePipelineStore(Path(args.state_db))
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
        f"state-db://{Path(args.state_db).name}?run_id={args.run_id}",
        bundle.normalized_rows,
        dict(decision_counts),
        len(bundle.candidate_pairs),
        cluster_count,
        len(bundle.golden_rows),
        len(filter_active_review_queue_rows(bundle.review_rows)),
        {
            key: bundle.run.summary[key]
            for key in ("refresh", "run_context")
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
            for key in ("refresh", "run_context")
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
        "duplicate_rate": None if manifest_path else args.duplicate_rate,
        "formats": None if manifest_path else args.formats,
        "refresh_mode": getattr(args, "refresh_mode", "full"),
    }


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


def _cmd_generate(args: argparse.Namespace) -> None:
    output_dir = Path(args.output)
    formats = _parse_formats(args.formats)
    result = generate_synthetic_sources(
        output_dir=output_dir,
        profile=args.profile,
        seed=args.seed,
        duplicate_rate=args.duplicate_rate,
        formats=formats,
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


def _cmd_state_db_upgrade(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    upgrade_sqlite_store(state_db, revision=args.revision)
    current_revision = current_sqlite_store_revision(state_db) or "uninitialized"
    print(f"state db upgraded: {state_db} (revision={current_revision})")


def _cmd_state_db_current(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    current_revision = current_sqlite_store_revision(state_db) or "uninitialized"
    print(f"state db revision: {current_revision} (head={head_revision()})")


def _resolve_review_case_run_id(args: argparse.Namespace, store: SQLitePipelineStore) -> str:
    run_id = args.run_id or store.latest_completed_run_id_with_review_cases()
    if run_id is None:
        raise FileNotFoundError(f"No completed persisted review-case runs found in {_require_state_db(args)}")
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


def _cmd_review_case_list(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    store = SQLitePipelineStore(state_db)
    run_id = _resolve_review_case_run_id(args, store)
    cases = store.list_review_cases(
        run_id=run_id,
        queue_status=args.status,
        assigned_to=args.assigned_to,
    )
    print(json.dumps([_serialize_review_case(case) for case in cases], indent=2, sort_keys=True))


def _cmd_review_case_update(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    store = SQLitePipelineStore(state_db)
    run_id = _resolve_review_case_run_id(args, store)
    updated = store.update_review_case(
        run_id=run_id,
        review_id=args.review_id,
        queue_status=args.status,
        assigned_to=args.assigned_to,
        operator_notes=args.notes,
    )
    print(json.dumps(_serialize_review_case(updated), indent=2, sort_keys=True))


def _cmd_publish_delivery(args: argparse.Namespace) -> None:
    state_db = _require_state_db(args)
    store = SQLitePipelineStore(state_db)
    run_id = args.run_id or store.latest_completed_run_id()
    if run_id is None:
        raise FileNotFoundError(f"No completed persisted runs found in {state_db}")

    bundle = store.load_run_bundle(run_id)
    published = publish_delivery_snapshot(
        bundle=bundle,
        state_db_path=state_db,
        output_root=Path(args.output_dir),
        contract_version=args.contract_version,
    )
    print(f"delivery snapshot published: {published.snapshot_dir}")
    print(f"delivery pointer updated: {published.current_pointer_path}")


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

    state_store = SQLitePipelineStore(Path(args.state_db)) if args.state_db else None
    run_started = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    run_key_args = _resolve_run_key_args(args, batch_id=manifest_batch_id)
    run_key = build_run_key(**run_key_args)
    run_id: str | None = None
    attempt_number = 0
    normalized_file = base / "data" / "normalized" / "normalized_person_records.csv"
    matches_file = base / "data" / "matches" / "candidate_scores.csv"
    clusters_file = base / "data" / "matches" / "entity_clusters.csv"
    golden_file = base / "data" / "golden" / "golden_person_records.csv"
    crosswalk_file = base / "data" / "golden" / "source_to_golden_crosswalk.csv"
    review_queue_file = base / "data" / "review_queue" / "manual_review_queue.csv"
    report_file = base / "data" / "exceptions" / "run_report.md"

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
            print(f"reused persisted completed run: {Path(args.state_db)} (run_id={bundle.run.run_id})")
            print("pipeline run complete")
            return
        run_id = start_decision.run_id
        attempt_number = start_decision.attempt_number

    try:
        config = _load_config(args.config_dir, args.environment)
        resolved_manifest: ResolvedBatchManifest | None = None
        input_mode = "manifest" if manifest_path else "synthetic"
        batch_id: str | None = None
        formats_value: str | None = None
        config_fingerprint = _config_fingerprint(config)
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
        if manifest_path:
            resolved_manifest = _resolve_manifest_inputs(manifest_path)
            batch_id = resolved_manifest.manifest.batch_id
            print(
                f"validated batch manifest: {resolved_manifest.manifest_path} "
                f"(batch_id={resolved_manifest.manifest.batch_id})"
            )
            for path in resolved_manifest.input_paths:
                print(f" - {path}")
            normalized_rows = _write_normalized_output(
                normalized_file,
                resolved_manifest.all_rows(),
                config,
            )
        else:
            formats = _parse_formats(args.formats)
            formats_value = ",".join(formats)
            batch_id = f"synthetic:{args.profile}:{args.seed}"
            synthetic_dir = base / "data" / "synthetic_sources"
            generate_synthetic_sources(
                output_dir=synthetic_dir,
                profile=args.profile,
                seed=args.seed,
                duplicate_rate=args.duplicate_rate,
                formats=formats,
            )
            normalized_rows = _write_normalized_output(
                normalized_file,
                _read_rows(_resolve_generated_person_input_paths(synthetic_dir, formats=formats)),
                config,
            )

        review_source_bundle = None
        review_overrides: dict[tuple[str, str], str] = {}
        forced_review_pairs: set[tuple[str, str]] = set()
        if state_store is not None and resolved_manifest is not None:
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
                    cluster_output_rows = incremental_result.cluster_output_rows
                    golden_rows = incremental_result.golden_rows
                    crosswalk_rows = incremental_result.crosswalk_rows
                    active_review_rows = incremental_result.active_review_rows
                    review_rows = incremental_result.review_rows
                    refresh_summary = incremental_result.metadata
                    _write_precomputed_match_outputs(matches_file, match_rows, blocking_metrics_rows)
                    _write_precomputed_cluster_output(clusters_file, cluster_output_rows)
                    _write_precomputed_review_queue_output(review_queue_file, active_review_rows)
                    _write_precomputed_golden_output(golden_file, golden_rows)
                    _write_precomputed_crosswalk_output(crosswalk_file, crosswalk_rows)
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

        if args.refresh_mode != "incremental" or previous_bundle is None or refresh_summary.get("fallback_to_full", False):
            match_rows, blocking_metrics_rows = _write_match_output(
                matches_file,
                normalized_rows,
                config,
                forced_pairs=forced_review_pairs,
                review_overrides=review_overrides,
            )
            clustered_rows = _write_cluster_output(clusters_file, normalized_rows, match_rows)
            active_review_rows, review_rows = _write_manual_review_output(
                review_queue_file,
                match_rows,
                previous_review_rows=review_source_bundle.review_rows if review_source_bundle is not None else None,
            )
            golden_rows = _write_golden_output(golden_file, clustered_rows, config)
            crosswalk_rows = _write_crosswalk_output(crosswalk_file, clustered_rows, golden_rows)
            if args.refresh_mode == "full":
                refresh_summary["fallback_to_full"] = False
                refresh_summary["reused_record_count"] = 0
                refresh_summary["affected_record_count"] = len(normalized_rows)
                refresh_summary["recalculated_candidate_pair_count"] = len(match_rows)
                refresh_summary["recalculated_cluster_count"] = len(
                    {row.get("cluster_id", "") for row in clustered_rows if row.get("cluster_id")}
                )
            else:
                refresh_summary["affected_record_count"] = len(normalized_rows)
                refresh_summary["recalculated_candidate_pair_count"] = len(match_rows)
                refresh_summary["recalculated_cluster_count"] = len(
                    {row.get("cluster_id", "") for row in clustered_rows if row.get("cluster_id")}
                )
                refresh_summary["reused_record_count"] = 0
        else:
            crosswalk_rows = incremental_result.crosswalk_rows

        decision_counts = Counter(str(row.get("decision", "")) for row in match_rows)
        summary = _write_quality_outputs(
            report_file,
            normalized_file,
            normalized_rows,
            candidate_pair_count=len(match_rows),
            decision_counts=dict(decision_counts),
            cluster_count=len({row.get("cluster_id", "") for row in clustered_rows if row.get("cluster_id")}),
            golden_record_count=len(golden_rows),
            review_queue_count=len(active_review_rows),
            summary_updates={
                "run_context": {
                    "input_mode": input_mode,
                    "batch_id": batch_id or "",
                    "manifest_path": str(resolved_manifest.manifest_path) if resolved_manifest else "",
                    "config_fingerprint": config_fingerprint,
                    "refresh_mode": args.refresh_mode,
                },
                "refresh": refresh_summary,
            },
        )
        if state_store is not None and run_id is not None:
            state_store.persist_run(
                metadata=PersistRunMetadata(
                    run_id=run_id,
                    run_key=run_key,
                    attempt_number=attempt_number,
                    batch_id=batch_id,
                    input_mode=input_mode,
                    manifest_path=str(resolved_manifest.manifest_path) if resolved_manifest else None,
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
            print(f"persisted run state: {Path(args.state_db)} (run_id={run_id})")
        print("pipeline run complete")
    except Exception as exc:
        if state_store is not None and run_id is not None:
            state_store.mark_run_failed(
                run_id=run_id,
                finished_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                failure_detail=str(exc),
            )
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl-identity-engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="Generate synthetic source records.")
    generate_parser.add_argument("--output", default="data/synthetic_sources")
    generate_parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    generate_parser.add_argument("--seed", default=42, type=int)
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

    report_parser = subparsers.add_parser("report", help="Produce run report.")
    report_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    report_parser.add_argument("--output", default="data/exceptions/run_report.md")
    report_parser.add_argument("--environment", default=None)
    report_parser.add_argument("--runtime-config", default=None)
    report_parser.add_argument(
        "--state-db",
        default=None,
        help="Path to a persisted SQLite state database for reloading a completed run.",
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
        help="Upgrade a persisted SQLite state database to the latest Alembic revision.",
    )
    state_db_upgrade_parser.add_argument("--environment", default=None)
    state_db_upgrade_parser.add_argument("--runtime-config", default=None)
    state_db_upgrade_parser.add_argument("--state-db", default=None)
    state_db_upgrade_parser.add_argument("--revision", default="head")
    state_db_upgrade_parser.set_defaults(func=_cmd_state_db_upgrade)

    state_db_current_parser = subparsers.add_parser(
        "state-db-current",
        help="Show the current Alembic revision for a persisted SQLite state database.",
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

    run_all_parser = subparsers.add_parser("run-all", help="Run all scaffold stages in sequence.")
    run_all_parser.add_argument("--base-dir", default=".")
    run_all_parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    run_all_parser.add_argument("--seed", default=42, type=int)
    run_all_parser.add_argument("--duplicate-rate", default=None, type=float)
    run_all_parser.add_argument("--formats", default="csv,parquet")
    run_all_parser.add_argument("--refresh-mode", default="full", choices=["full", "incremental"])
    run_all_parser.add_argument("--environment", default=None)
    run_all_parser.add_argument("--runtime-config", default=None)
    run_all_parser.add_argument(
        "--state-db",
        default=None,
        help="Path to a SQLite database where completed run state should be persisted.",
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
