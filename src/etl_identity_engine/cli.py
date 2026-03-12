"""Command-line entrypoint for the ETL Identity Engine scaffold."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from etl_identity_engine.generate.synth_generator import generate_synthetic_sources
from etl_identity_engine.io.read import read_csv_dicts
from etl_identity_engine.io.write import write_csv_dicts, write_markdown
from etl_identity_engine.matching.blocking import generate_candidates
from etl_identity_engine.matching.scoring import score_pair
from etl_identity_engine.normalize.addresses import normalize_address
from etl_identity_engine.normalize.dates import normalize_date
from etl_identity_engine.normalize.names import normalize_name
from etl_identity_engine.normalize.phones import normalize_phone
from etl_identity_engine.quality.dq_checks import summarize_missing_fields
from etl_identity_engine.survivorship.rules_engine import merge_records


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
    input_file = Path(args.input)
    output_file = Path(args.output)
    rows = read_csv_dicts(input_file)

    normalized_rows = []
    for row in rows:
        canonical_name = normalize_name(
            f"{row.get('first_name', '')} {row.get('last_name', '')}"
        )
        normalized_rows.append(
            {
                **row,
                "canonical_name": canonical_name,
                "canonical_dob": normalize_date(row.get("dob", "")) or "",
                "canonical_address": normalize_address(row.get("address", "")),
                "canonical_phone": normalize_phone(row.get("phone", "")),
            }
        )

    write_csv_dicts(output_file, normalized_rows)
    print(f"normalized rows written: {output_file}")


def _cmd_match(args: argparse.Namespace) -> None:
    input_file = Path(args.input)
    output_file = Path(args.output)
    rows = read_csv_dicts(input_file)

    pairs = generate_candidates(rows)
    scored_rows = []
    for left, right in pairs:
        scored_rows.append(
            {
                "left_id": left.get("source_record_id", ""),
                "right_id": right.get("source_record_id", ""),
                "score": score_pair(left, right),
            }
        )

    write_csv_dicts(output_file, scored_rows)
    print(f"candidate scores written: {output_file}")


def _cmd_golden(args: argparse.Namespace) -> None:
    input_file = Path(args.input)
    output_file = Path(args.output)
    rows = read_csv_dicts(input_file)

    # Scaffold behavior: merge all rows into one sample golden record.
    if rows:
        golden = merge_records(rows)
        write_csv_dicts(output_file, [golden])
    else:
        write_csv_dicts(output_file, [])
    print(f"golden output written: {output_file}")


def _cmd_report(args: argparse.Namespace) -> None:
    input_file = Path(args.input)
    output_file = Path(args.output)
    rows = read_csv_dicts(input_file)
    dq_summary = summarize_missing_fields(rows)
    lines = [
        "# Pipeline Report",
        "",
        f"- Input file: `{input_file}`",
        f"- Total records: `{len(rows)}`",
        "",
        "## Missing Field Counts",
    ]
    for key, value in sorted(dq_summary.items()):
        lines.append(f"- `{key}`: `{value}`")

    write_markdown(output_file, "\n".join(lines))
    print(f"report written: {output_file}")


def _cmd_run_all(args: argparse.Namespace) -> None:
    base = Path(args.base_dir)
    synthetic_dir = base / "data" / "synthetic_sources"
    normalized_file = base / "data" / "normalized" / "normalized_person_records.csv"
    matches_file = base / "data" / "matches" / "candidate_scores.csv"
    golden_file = base / "data" / "golden" / "golden_person_records.csv"
    report_file = base / "data" / "exceptions" / "run_report.md"

    generate_synthetic_sources(
        output_dir=synthetic_dir,
        profile=args.profile,
        seed=args.seed,
        duplicate_rate=args.duplicate_rate,
        formats=("csv", "parquet"),
    )
    _cmd_normalize(
        argparse.Namespace(
            input=synthetic_dir / "person_source_a.csv",
            output=normalized_file,
        )
    )
    _cmd_match(argparse.Namespace(input=normalized_file, output=matches_file))
    _cmd_golden(argparse.Namespace(input=normalized_file, output=golden_file))
    _cmd_report(argparse.Namespace(input=normalized_file, output=report_file))
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
    normalize_parser.add_argument("--input", default="data/synthetic_sources/person_source_a.csv")
    normalize_parser.add_argument(
        "--output",
        default="data/normalized/normalized_person_records.csv",
    )
    normalize_parser.set_defaults(func=_cmd_normalize)

    match_parser = subparsers.add_parser("match", help="Generate and score candidate pairs.")
    match_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    match_parser.add_argument("--output", default="data/matches/candidate_scores.csv")
    match_parser.set_defaults(func=_cmd_match)

    golden_parser = subparsers.add_parser("golden", help="Build golden records.")
    golden_parser.add_argument("--input", default="data/normalized/normalized_person_records.csv")
    golden_parser.add_argument("--output", default="data/golden/golden_person_records.csv")
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
    run_all_parser.set_defaults(func=_cmd_run_all)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
