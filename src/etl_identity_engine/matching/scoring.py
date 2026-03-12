"""Weighted scoring for candidate pairs."""

from __future__ import annotations


def score_pair(left: dict[str, str], right: dict[str, str]) -> float:
    score = 0.0
    if left.get("canonical_name", "").strip() == right.get("canonical_name", "").strip():
        score += 0.5
    if left.get("canonical_dob", "").strip() == right.get("canonical_dob", "").strip():
        score += 0.3
    if left.get("canonical_phone", "").strip() == right.get("canonical_phone", "").strip():
        score += 0.1
    if left.get("canonical_address", "").strip() == right.get("canonical_address", "").strip():
        score += 0.1
    return round(score, 4)

