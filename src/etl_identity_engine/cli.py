"""Command-line entrypoint for the ETL Identity Engine scaffold."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Sequence

from etl_identity_engine.generate.synth_generator import generate_synthetic_sources
from etl_identity_engine.io.read import read_csv_dicts
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
                ),
            }
        )
    return normalized_rows


def _read_rows(paths: Sequence[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in paths:
        rows.extend(read_csv_dicts(path))
    return rows


def _discover_normalize_input_paths(input_dir: Path) -> tuple[Path, ...]:
    input_paths = tuple(sorted(input_dir.glob("person_source_*.csv")))
    if not input_paths:
        raise FileNotFoundError(
            f"No normalization input files found in {input_dir} matching person_source_*.csv"
        )
    return input_paths


def _resolve_normalize_input_paths(args: argparse.Namespace) -> tuple[Path, ...]:
    explicit_inputs = tuple(Path(path) for path in (args.input or []))
    if explicit_inputs:
        return explicit_inputs
    return _discover_normalize_input_paths(Path(args.input_dir))


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
    formats = tuple(part.strip() for part in args.formats.split(",") if part.strip())
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
    _write_match_output(output_file, read_csv_dicts(input_file), _load_config(args.config_dir))


def _cmd_golden(args: argparse.Namespace) -> None:
    input_file = Path(args.input)
    output_file = Path(args.output)
    _write_golden_output(output_file, read_csv_dicts(input_file), _load_config(args.config_dir))


def _cmd_report(args: argparse.Namespace) -> None:
    input_file = Path(args.input)
    output_file = Path(args.output)
    rows = read_csv_dicts(input_file)
    _write_quality_outputs(output_file, input_file, rows)


def _cmd_run_all(args: argparse.Namespace) -> None:
    base = Path(args.base_dir)
    config = _load_config(args.config_dir)
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
        formats=("csv", "parquet"),
    )
    normalized_rows = _write_normalized_output(
        normalized_file,
        _read_rows(
            (
                synthetic_dir / "person_source_a.csv",
                synthetic_dir / "person_source_b.csv",
            )
        ),
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
        help="Path to a source CSV file. Repeat to normalize multiple explicit inputs.",
    )
    normalize_parser.add_argument(
        "--input-dir",
        default="data/synthetic_sources",
        help="Directory to scan for person_source_*.csv files when --input is not provided.",
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

    golden_parser = subparsers.add_parser("golden", help="Build golden records.")
    golden_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    golden_parser.add_argument("--output", default="data/golden/golden_person_records.csv")
    golden_parser.add_argument("--config-dir", default=None)
    golden_parser.set_defaults(func=_cmd_golden)

    report_parser = subparsers.add_parser("report", help="Produce run report.")
    report_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    report_parser.add_argument("--output", default="data/exceptions/run_report.md")
    report_parser.set_defaults(func=_cmd_report)

    run_all_parser = subparsers.add_parser("run-all", help="Run all scaffold stages in sequence.")
    run_all_parser.add_argument("--base-dir", default=".")
    run_all_parser.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    run_all_parser.add_argument("--seed", default=42, type=int)
    run_all_parser.add_argument("--duplicate-rate", default=None, type=float)
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
