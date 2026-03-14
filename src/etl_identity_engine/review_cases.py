"""Manual review case lifecycle helpers."""

from __future__ import annotations


REVIEW_CASE_STATUSES = ("pending", "approved", "rejected", "deferred")
REVIEW_CASE_TERMINAL_STATUSES = frozenset({"approved", "rejected"})
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
