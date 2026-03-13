"""Command-line entrypoint for the ETL Identity Engine scaffold."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Sequence

from etl_identity_engine.generate.synth_generator import generate_synthetic_sources
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
    ENTITY_CLUSTER_HEADERS,
    EXCEPTION_HEADERS,
    GOLDEN_HEADERS,
    MANUAL_REVIEW_HEADERS,
    MATCH_SCORE_HEADERS,
    NORMALIZED_HEADERS,
)
from etl_identity_engine.quality.exceptions import (
    build_run_report_markdown,
    build_run_summary,
    extract_exception_rows,
)
from etl_identity_engine.runtime_config import PipelineConfig, load_pipeline_config
from etl_identity_engine.survivorship.rules_engine import build_golden_records


def _load_config(config_dir: str | None) -> PipelineConfig:
    return load_pipeline_config(Path(config_dir) if config_dir else None)


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


def _collect_report_context(
    args: argparse.Namespace,
) -> tuple[Path, list[dict[str, str]], dict[str, int], int, int, int, int]:
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
    return (
        input_file,
        normalized_rows,
        dict(decision_counts),
        len(match_rows),
        cluster_count,
        len(golden_rows),
        len(review_rows),
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
) -> tuple[list[dict[str, str | float]], list[BlockingPassMetric]]:
    blocking_passes = [blocking_pass.fields for blocking_pass in config.matching.blocking_passes]
    blocking_pass_names = [blocking_pass.name for blocking_pass in config.matching.blocking_passes]
    pairs, blocking_metrics = generate_candidates_with_metrics(
        rows,
        blocking_passes=blocking_passes,
        pass_names=blocking_pass_names,
    )
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
) -> tuple[list[dict[str, str | float]], list[dict[str, str | int]]]:
    scored_rows, blocking_metrics = _build_match_rows(rows, config)
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


def _build_manual_review_rows(
    match_rows: list[dict[str, str | float]],
) -> list[dict[str, str | float]]:
    review_candidates = [row for row in match_rows if row.get("decision") == "manual_review"]
    ordered_candidates = sorted(
        review_candidates,
        key=lambda row: (
            -float(row.get("score", 0.0)),
            str(row.get("left_id", "")),
            str(row.get("right_id", "")),
        ),
    )

    review_rows: list[dict[str, str | float]] = []
    for index, row in enumerate(ordered_candidates, start=1):
        review_rows.append(
            {
                "review_id": f"REV-{index:05d}",
                "left_id": row.get("left_id", ""),
                "right_id": row.get("right_id", ""),
                "score": row.get("score", 0.0),
                "reason_codes": row.get("reason_trace", ""),
                "top_contributing_match_signals": row.get("matched_fields", ""),
                "queue_status": "pending",
            }
        )
    return review_rows


def _write_manual_review_output(
    output_file: Path,
    match_rows: list[dict[str, str | float]],
) -> list[dict[str, str | float]]:
    review_rows = _build_manual_review_rows(match_rows)
    write_csv_dicts(output_file, review_rows, fieldnames=MANUAL_REVIEW_HEADERS)
    print(f"manual review queue written: {output_file}")
    return review_rows


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


def _write_quality_outputs(
    output_file: Path,
    input_file: Path,
    rows: list[dict[str, str]],
    *,
    candidate_pair_count: int = 0,
    decision_counts: dict[str, int] | None = None,
    cluster_count: int = 0,
    golden_record_count: int = 0,
    review_queue_count: int = 0,
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
    write_markdown(output_file, build_run_report_markdown(str(input_file), summary))

    summary_path = output_file.with_name("run_summary.json")
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    print(f"report written: {output_file}")
    print(f"run summary written: {summary_path}")
    return summary


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
    input_paths = _resolve_normalize_input_paths(args)
    print(f"normalizing {len(input_paths)} input file(s)")
    for path in input_paths:
        print(f" - {path}")
    _write_normalized_output(output_file, _read_rows(input_paths), _load_config(args.config_dir))


def _cmd_match(args: argparse.Namespace) -> None:
    input_file = Path(args.input)
    output_file = Path(args.output)
    _write_match_output(output_file, read_dict_rows(input_file), _load_config(args.config_dir))


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
    _write_golden_output(output_file, _resolve_golden_input_rows(args), _load_config(args.config_dir))


def _cmd_report(args: argparse.Namespace) -> None:
    (
        input_file,
        rows,
        decision_counts,
        candidate_pair_count,
        cluster_count,
        golden_record_count,
        review_queue_count,
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
    )


def _cmd_run_all(args: argparse.Namespace) -> None:
    base = Path(args.base_dir)
    config = _load_config(args.config_dir)
    formats = _parse_formats(args.formats)
    synthetic_dir = base / "data" / "synthetic_sources"
    normalized_file = base / "data" / "normalized" / "normalized_person_records.csv"
    matches_file = base / "data" / "matches" / "candidate_scores.csv"
    clusters_file = base / "data" / "matches" / "entity_clusters.csv"
    golden_file = base / "data" / "golden" / "golden_person_records.csv"
    crosswalk_file = base / "data" / "golden" / "source_to_golden_crosswalk.csv"
    review_queue_file = base / "data" / "review_queue" / "manual_review_queue.csv"
    report_file = base / "data" / "exceptions" / "run_report.md"

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
    match_rows, _ = _write_match_output(matches_file, normalized_rows, config)
    clustered_rows = _write_cluster_output(clusters_file, normalized_rows, match_rows)
    review_rows = _write_manual_review_output(review_queue_file, match_rows)
    golden_rows = _write_golden_output(golden_file, clustered_rows, config)
    _write_crosswalk_output(crosswalk_file, clustered_rows, golden_rows)

    decision_counts = Counter(str(row.get("decision", "")) for row in match_rows)
    _write_quality_outputs(
        report_file,
        normalized_file,
        normalized_rows,
        candidate_pair_count=len(match_rows),
        decision_counts=dict(decision_counts),
        cluster_count=len({row.get("cluster_id", "") for row in clustered_rows if row.get("cluster_id")}),
        golden_record_count=len(golden_rows),
        review_queue_count=len(review_rows),
    )
    print("pipeline run complete")


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
        default="data/synthetic_sources",
        help=(
            "Directory to scan for person_source_*.csv files, or person_source_*.parquet "
            "when CSV files are absent, if --input is not provided."
        ),
    )
    normalize_parser.add_argument(
        "--output",
        default="data/normalized/normalized_person_records.csv",
    )
    normalize_parser.add_argument("--config-dir", default=None)
    normalize_parser.set_defaults(func=_cmd_normalize)

    match_parser = subparsers.add_parser("match", help="Generate and score candidate pairs.")
    match_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    match_parser.add_argument("--output", default="data/matches/candidate_scores.csv")
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
    golden_parser.add_argument("--config-dir", default=None)
    golden_parser.set_defaults(func=_cmd_golden)

    report_parser = subparsers.add_parser("report", help="Produce run report.")
    report_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    report_parser.add_argument("--output", default="data/exceptions/run_report.md")
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

    run_all_parser = subparsers.add_parser("run-all", help="Run all scaffold stages in sequence.")
    run_all_parser.add_argument("--base-dir", default=".")
    run_all_parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    run_all_parser.add_argument("--seed", default=42, type=int)
    run_all_parser.add_argument("--duplicate-rate", default=None, type=float)
    run_all_parser.add_argument("--formats", default="csv,parquet")
    run_all_parser.add_argument("--config-dir", default=None)
    run_all_parser.set_defaults(func=_cmd_run_all)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
