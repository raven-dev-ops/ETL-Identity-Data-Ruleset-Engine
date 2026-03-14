"""Manual review case lifecycle helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import re

from etl_identity_engine.output_contracts import MANUAL_REVIEW_HEADERS


REVIEW_CASE_STATUSES = ("pending", "approved", "rejected", "deferred")
REVIEW_CASE_TERMINAL_STATUSES = frozenset({"approved", "rejected"})
REVIEW_CASE_OVERRIDE_STATUSES = frozenset({"approved", "rejected"})
REVIEW_CASE_ACTIVE_STATUSES = frozenset({"pending", "deferred"})
REVIEW_CASE_ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "pending": frozenset({"pending", "approved", "rejected", "deferred"}),
    "approved": frozenset({"approved", "pending"}),
    "rejected": frozenset({"rejected", "pending"}),
    "deferred": frozenset({"deferred", "pending", "approved", "rejected"}),
}


def validate_review_case_status(status: str) -> str:
    normalized = status.strip().lower()
    if normalized not in REVIEW_CASE_STATUSES:
        raise ValueError(
            f"Unsupported review case status {status!r}; expected one of {REVIEW_CASE_STATUSES}"
        )
    return normalized


def validate_review_case_transition(current_status: str, target_status: str) -> str:
    current = validate_review_case_status(current_status)
    target = validate_review_case_status(target_status)
    allowed = REVIEW_CASE_ALLOWED_TRANSITIONS[current]
    if target not in allowed:
        raise ValueError(
            f"Review case cannot transition from {current!r} to {target!r}; allowed targets: {sorted(allowed)}"
        )
    return target


def review_pair_key(left_id: str, right_id: str) -> tuple[str, str]:
    return tuple(sorted((left_id.strip(), right_id.strip())))


def review_case_pair_key(review_case: Mapping[str, object]) -> tuple[str, str]:
    return review_pair_key(
        str(review_case.get("left_id", "")),
        str(review_case.get("right_id", "")),
    )


def build_review_override_map(
    review_cases: Sequence[Mapping[str, object]],
) -> dict[tuple[str, str], str]:
    overrides: dict[tuple[str, str], str] = {}
    for review_case in review_cases:
        pair_key = review_case_pair_key(review_case)
        if not all(pair_key):
            continue
        status = validate_review_case_status(str(review_case.get("queue_status", "pending")))
        if status in REVIEW_CASE_OVERRIDE_STATUSES:
            overrides[pair_key] = status
    return overrides


def apply_review_decisions(
    match_rows: Sequence[Mapping[str, object]],
    review_overrides: Mapping[tuple[str, str], str],
) -> list[dict[str, str | float]]:
    overridden_rows: list[dict[str, str | float]] = []
    for row in match_rows:
        resolved_row = dict(row)
        override_status = review_overrides.get(
            review_pair_key(
                str(resolved_row.get("left_id", "")),
                str(resolved_row.get("right_id", "")),
            )
        )
        if override_status == "approved":
            resolved_row["decision"] = "auto_merge"
            reason_trace = str(resolved_row.get("reason_trace", "")).strip(";")
            resolved_row["reason_trace"] = (
                f"{reason_trace};review_case_approved_override"
                if reason_trace
                else "review_case_approved_override"
            )
        elif override_status == "rejected":
            resolved_row["decision"] = "no_match"
            reason_trace = str(resolved_row.get("reason_trace", "")).strip(";")
            resolved_row["reason_trace"] = (
                f"{reason_trace};review_case_rejected_override"
                if reason_trace
                else "review_case_rejected_override"
            )
        overridden_rows.append(resolved_row)
    return overridden_rows


def filter_active_review_queue_rows(
    review_cases: Sequence[Mapping[str, object]],
) -> list[dict[str, str | float]]:
    active_rows: list[dict[str, str | float]] = []
    for review_case in review_cases:
        status = validate_review_case_status(str(review_case.get("queue_status", "pending")))
        if status not in REVIEW_CASE_ACTIVE_STATUSES:
            continue
        active_rows.append(
            {
                header: review_case.get(header, "")
                for header in MANUAL_REVIEW_HEADERS
            }
        )
    return active_rows


def build_review_case_rows(
    match_rows: Sequence[Mapping[str, object]],
    *,
    previous_review_cases: Sequence[Mapping[str, object]] = (),
) -> tuple[list[dict[str, str | float]], list[dict[str, str | float]]]:
    previous_cases_by_pair: dict[tuple[str, str], dict[str, str]] = {}
    next_review_number = 1
    review_id_pattern = re.compile(r"^REV-(\d+)$")

    for review_case in previous_review_cases:
        pair_key = review_case_pair_key(review_case)
        if not all(pair_key):
            continue
        previous_cases_by_pair[pair_key] = {
            key: "" if review_case.get(key) is None else str(review_case.get(key, ""))
            for key in (
                "review_id",
                "left_id",
                "right_id",
                "queue_status",
                "assigned_to",
                "operator_notes",
                "created_at_utc",
                "updated_at_utc",
                "resolved_at_utc",
            )
        }
        match = review_id_pattern.match(previous_cases_by_pair[pair_key]["review_id"])
        if match is not None:
            next_review_number = max(next_review_number, int(match.group(1)) + 1)

    persisted_rows: list[dict[str, str | float]] = []
    for row in sorted(
        match_rows,
        key=lambda item: (
            -float(item.get("score", 0.0) or 0.0),
            str(item.get("left_id", "")),
            str(item.get("right_id", "")),
        ),
    ):
        pair_key = review_pair_key(str(row.get("left_id", "")), str(row.get("right_id", "")))
        if not all(pair_key):
            continue
        previous_case = previous_cases_by_pair.get(pair_key)
        decision = str(row.get("decision", ""))

        if decision == "manual_review":
            if previous_case and previous_case.get("queue_status") in REVIEW_CASE_ACTIVE_STATUSES:
                review_id = previous_case["review_id"]
                queue_status = previous_case["queue_status"]
                assigned_to = previous_case["assigned_to"]
                operator_notes = previous_case["operator_notes"]
                created_at_utc = previous_case["created_at_utc"]
            else:
                review_id = f"REV-{next_review_number:05d}"
                next_review_number += 1
                queue_status = "pending"
                assigned_to = ""
                operator_notes = ""
                created_at_utc = ""

            persisted_rows.append(
                {
                    "review_id": review_id,
                    "left_id": row.get("left_id", ""),
                    "right_id": row.get("right_id", ""),
                    "score": row.get("score", 0.0),
                    "reason_codes": row.get("reason_trace", ""),
                    "top_contributing_match_signals": row.get("matched_fields", ""),
                    "queue_status": queue_status,
                    "assigned_to": assigned_to,
                    "operator_notes": operator_notes,
                    "created_at_utc": created_at_utc,
                    "updated_at_utc": previous_case.get("updated_at_utc", "") if previous_case else "",
                    "resolved_at_utc": "",
                }
            )
            continue

        if previous_case and previous_case.get("queue_status") in REVIEW_CASE_OVERRIDE_STATUSES:
            persisted_rows.append(
                {
                    "review_id": previous_case["review_id"],
                    "left_id": row.get("left_id", ""),
                    "right_id": row.get("right_id", ""),
                    "score": row.get("score", 0.0),
                    "reason_codes": row.get("reason_trace", ""),
                    "top_contributing_match_signals": row.get("matched_fields", ""),
                    "queue_status": previous_case["queue_status"],
                    "assigned_to": previous_case["assigned_to"],
                    "operator_notes": previous_case["operator_notes"],
                    "created_at_utc": previous_case["created_at_utc"],
                    "updated_at_utc": previous_case["updated_at_utc"],
                    "resolved_at_utc": previous_case["resolved_at_utc"],
                }
            )

    return filter_active_review_queue_rows(persisted_rows), persisted_rows
