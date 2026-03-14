"""Exception extraction and run-summary helpers."""

from __future__ import annotations

from etl_identity_engine.quality.dq_checks import summarize_missing_fields


def extract_exception_rows(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    invalid_dobs: list[dict[str, str]] = []
    malformed_phones: list[dict[str, str]] = []
    normalization_failures: list[dict[str, str]] = []

    for row in rows:
        record_context = {
            "source_record_id": row.get("source_record_id", ""),
            "person_entity_id": row.get("person_entity_id", ""),
            "source_system": row.get("source_system", ""),
        }

        raw_dob = row.get("dob", "").strip()
        canonical_dob = row.get("canonical_dob", "").strip()
        if raw_dob and not canonical_dob:
            invalid_dobs.append(
                {
                    **record_context,
                    "field_name": "dob",
                    "raw_value": raw_dob,
                    "canonical_value": canonical_dob,
                    "reason_code": "invalid_dob",
                }
            )

        raw_phone = row.get("phone", "").strip()
        canonical_phone = row.get("canonical_phone", "").strip()
        canonical_phone_digits = "".join(ch for ch in canonical_phone if ch.isdigit())
        if raw_phone and len(canonical_phone_digits) != 10:
            malformed_phones.append(
                {
                    **record_context,
                    "field_name": "phone",
                    "raw_value": raw_phone,
                    "canonical_value": canonical_phone,
                    "reason_code": "malformed_phone",
                }
            )

        raw_name = " ".join(
            part.strip() for part in (row.get("first_name", ""), row.get("last_name", "")) if part.strip()
        )
        if raw_name and not row.get("canonical_name", "").strip():
            normalization_failures.append(
                {
                    **record_context,
                    "field_name": "canonical_name",
                    "raw_value": raw_name,
                    "canonical_value": row.get("canonical_name", ""),
                    "reason_code": "canonical_name_blank",
                }
            )

        raw_address = row.get("address", "").strip()
        if raw_address and not row.get("canonical_address", "").strip():
            normalization_failures.append(
                {
                    **record_context,
                    "field_name": "canonical_address",
                    "raw_value": raw_address,
                    "canonical_value": row.get("canonical_address", ""),
                    "reason_code": "canonical_address_blank",
                }
            )

    return {
        "invalid_dobs": invalid_dobs,
        "malformed_phones": malformed_phones,
        "normalization_failures": normalization_failures,
    }


def _build_completeness_metrics(rows: list[dict[str, str]]) -> dict[str, int]:
    raw_name_present = sum(
        1
        for row in rows
        if any(part.strip() for part in (row.get("first_name", ""), row.get("last_name", "")))
    )
    return {
        "raw_name_present": raw_name_present,
        "canonical_name_present": sum(1 for row in rows if row.get("canonical_name", "").strip()),
        "raw_dob_present": sum(1 for row in rows if row.get("dob", "").strip()),
        "canonical_dob_present": sum(1 for row in rows if row.get("canonical_dob", "").strip()),
        "raw_phone_present": sum(1 for row in rows if row.get("phone", "").strip()),
        "canonical_phone_present": sum(1 for row in rows if row.get("canonical_phone", "").strip()),
    }


def _build_before_after_completeness(completeness: dict[str, int]) -> dict[str, dict[str, int]]:
    return {
        "name": {
            "before": completeness["raw_name_present"],
            "after": completeness["canonical_name_present"],
            "delta": completeness["canonical_name_present"] - completeness["raw_name_present"],
        },
        "dob": {
            "before": completeness["raw_dob_present"],
            "after": completeness["canonical_dob_present"],
            "delta": completeness["canonical_dob_present"] - completeness["raw_dob_present"],
        },
        "phone": {
            "before": completeness["raw_phone_present"],
            "after": completeness["canonical_phone_present"],
            "delta": completeness["canonical_phone_present"] - completeness["raw_phone_present"],
        },
    }


def _build_duplicate_reduction_metrics(
    *,
    total_records: int,
    cluster_count: int,
    golden_record_count: int,
) -> dict[str, int | float]:
    after_record_count = golden_record_count or cluster_count or total_records
    after_record_count = min(after_record_count, total_records) if total_records else 0
    records_consolidated = max(total_records - after_record_count, 0)
    return {
        "before_record_count": total_records,
        "after_record_count": after_record_count,
        "records_consolidated": records_consolidated,
        "reduction_ratio": round(records_consolidated / total_records, 4) if total_records else 0.0,
    }


def build_run_summary(
    rows: list[dict[str, str]],
    *,
    exception_rows: dict[str, list[dict[str, str]]] | None = None,
    candidate_pair_count: int = 0,
    decision_counts: dict[str, int] | None = None,
    cluster_count: int = 0,
    golden_record_count: int = 0,
    review_queue_count: int = 0,
) -> dict[str, object]:
    exceptions = exception_rows or extract_exception_rows(rows)
    decisions = decision_counts or {}
    completeness = _build_completeness_metrics(rows)
    before_after_completeness = _build_before_after_completeness(completeness)
    duplicate_reduction = _build_duplicate_reduction_metrics(
        total_records=len(rows),
        cluster_count=cluster_count,
        golden_record_count=golden_record_count,
    )

    return {
        "total_records": len(rows),
        "missing_field_counts": summarize_missing_fields(rows),
        "exception_counts": {
            exception_type: len(items) for exception_type, items in sorted(exceptions.items())
        },
        "candidate_pair_count": candidate_pair_count,
        "decision_counts": {
            "auto_merge": decisions.get("auto_merge", 0),
            "manual_review": decisions.get("manual_review", 0),
            "no_match": decisions.get("no_match", 0),
        },
        "cluster_count": cluster_count,
        "golden_record_count": golden_record_count,
        "review_queue_count": review_queue_count,
        "completeness": completeness,
        "before_after_completeness": before_after_completeness,
        "duplicate_reduction": duplicate_reduction,
    }


def build_run_report_markdown(input_path: str, summary: dict[str, object]) -> str:
    exception_counts = summary.get("exception_counts", {})
    decision_counts = summary.get("decision_counts", {})
    completeness = summary.get("completeness", {})
    before_after_completeness = summary.get("before_after_completeness", {})
    duplicate_reduction = summary.get("duplicate_reduction", {})
    missing_field_counts = summary.get("missing_field_counts", {})
    refresh = summary.get("refresh", {})
    run_context = summary.get("run_context", {})
    performance = summary.get("performance", {})
    phase_metrics = performance.get("phase_metrics", {}) if isinstance(performance, dict) else {}

    lines = [
        "# Pipeline Report",
        "",
        f"- Input file: `{input_path}`",
        f"- Input mode: `{run_context.get('input_mode', 'unknown')}`",
        f"- Batch ID: `{run_context.get('batch_id', '')}`",
        f"- Refresh mode: `{refresh.get('mode', run_context.get('refresh_mode', 'full'))}`",
        f"- Total records: `{summary.get('total_records', 0)}`",
        f"- Candidate pairs: `{summary.get('candidate_pair_count', 0)}`",
        f"- Auto-merge pairs: `{decision_counts.get('auto_merge', 0)}`",
        f"- Manual review pairs: `{decision_counts.get('manual_review', 0)}`",
        f"- No-match pairs: `{decision_counts.get('no_match', 0)}`",
        f"- Cluster count: `{summary.get('cluster_count', 0)}`",
        f"- Golden record count: `{summary.get('golden_record_count', 0)}`",
        f"- Review queue count: `{summary.get('review_queue_count', 0)}`",
        "",
    ]

    if refresh:
        lines.extend(
            [
                "## Refresh",
                f"- `predecessor_run_id`: `{refresh.get('predecessor_run_id', '')}`",
                f"- `fallback_to_full`: `{refresh.get('fallback_to_full', False)}`",
                f"- `affected_record_count`: `{refresh.get('affected_record_count', 0)}`",
                f"- `reused_record_count`: `{refresh.get('reused_record_count', 0)}`",
                f"- `inserted_record_count`: `{refresh.get('inserted_record_count', 0)}`",
                f"- `changed_record_count`: `{refresh.get('changed_record_count', 0)}`",
                f"- `removed_record_count`: `{refresh.get('removed_record_count', 0)}`",
                f"- `recalculated_cluster_count`: `{refresh.get('recalculated_cluster_count', 0)}`",
                f"- `reused_cluster_count`: `{refresh.get('reused_cluster_count', 0)}`",
                "",
            ]
        )

    if performance:
        lines.extend(
            [
                "## Performance",
                f"- `total_duration_seconds`: `{performance.get('total_duration_seconds', 0.0)}`",
            ]
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
        lines.append("")

    lines.extend(
        [
            "## Completeness",
            f"- `raw_name_present`: `{completeness.get('raw_name_present', 0)}`",
            f"- `canonical_name_present`: `{completeness.get('canonical_name_present', 0)}`",
            f"- `raw_dob_present`: `{completeness.get('raw_dob_present', 0)}`",
            f"- `canonical_dob_present`: `{completeness.get('canonical_dob_present', 0)}`",
            f"- `raw_phone_present`: `{completeness.get('raw_phone_present', 0)}`",
            f"- `canonical_phone_present`: `{completeness.get('canonical_phone_present', 0)}`",
            "",
            "## Before/After Completeness",
        ]
    )

    for field_name in ("name", "dob", "phone"):
        field_metrics = before_after_completeness.get(field_name, {})
        lines.append(
            f"- `{field_name}`: before=`{field_metrics.get('before', 0)}`, "
            f"after=`{field_metrics.get('after', 0)}`, "
            f"delta=`{field_metrics.get('delta', 0)}`"
        )

    lines.extend(
        [
            "",
            "## Duplicate Reduction",
            f"- `before_record_count`: `{duplicate_reduction.get('before_record_count', 0)}`",
            f"- `after_record_count`: `{duplicate_reduction.get('after_record_count', 0)}`",
            f"- `records_consolidated`: `{duplicate_reduction.get('records_consolidated', 0)}`",
            f"- `reduction_ratio`: `{duplicate_reduction.get('reduction_ratio', 0.0)}`",
            "",
        "## Missing Field Counts",
        ]
    )

    for key, value in sorted(missing_field_counts.items()):
        lines.append(f"- `{key}`: `{value}`")

    lines.extend(["", "## Exception Counts"])
    for key, value in sorted(exception_counts.items()):
        lines.append(f"- `{key}`: `{value}`")

    return "\n".join(lines)
