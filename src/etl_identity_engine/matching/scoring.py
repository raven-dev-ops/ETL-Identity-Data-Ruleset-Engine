"""Weighted scoring for candidate pairs."""

from __future__ import annotations

from dataclasses import dataclass


DEFAULT_WEIGHTS = {
    "canonical_name": 0.5,
    "canonical_dob": 0.3,
    "canonical_phone": 0.1,
    "canonical_address": 0.1,
}


def _matching_non_empty_field(
    left: dict[str, str],
    right: dict[str, str],
    field_name: str,
) -> bool:
    left_value = left.get(field_name, "").strip()
    right_value = right.get(field_name, "").strip()
    return bool(left_value and right_value and left_value == right_value)


@dataclass(frozen=True)
class PairScoreDetail:
    score: float
    matched_fields: tuple[str, ...]
    reason_trace: tuple[str, ...]


def explain_pair_score(
    left: dict[str, str],
    right: dict[str, str],
    *,
    weights: dict[str, float] | None = None,
) -> PairScoreDetail:
    effective_weights = weights or DEFAULT_WEIGHTS
    matched_fields: list[str] = []
    reason_trace: list[str] = []
    score = 0.0

    for field_name, weight in effective_weights.items():
        if _matching_non_empty_field(left, right, field_name):
            score += weight
            matched_fields.append(field_name)
            reason_trace.append(f"{field_name}:{weight:g}")

    if not reason_trace:
        reason_trace.append("no_weighted_matches")

    return PairScoreDetail(
        score=round(score, 4),
        matched_fields=tuple(matched_fields),
        reason_trace=tuple(reason_trace),
    )


def score_pair(
    left: dict[str, str],
    right: dict[str, str],
    *,
    weights: dict[str, float] | None = None,
) -> float:
    return explain_pair_score(left, right, weights=weights).score


def classify_score(
    score: float,
    *,
    auto_merge: float,
    manual_review_min: float,
    no_match_max: float,
) -> str:
    if score >= auto_merge:
        return "auto_merge"
    if score >= manual_review_min:
        return "manual_review"
    if score <= no_match_max:
        return "no_match"
    return "manual_review"

