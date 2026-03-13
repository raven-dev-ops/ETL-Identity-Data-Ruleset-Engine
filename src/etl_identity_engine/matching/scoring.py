"""Weighted scoring for candidate pairs."""

from __future__ import annotations

from dataclasses import dataclass
import re


DEFAULT_WEIGHTS = {
    "canonical_name": 0.5,
    "canonical_dob": 0.3,
    "canonical_phone": 0.1,
    "canonical_address": 0.1,
}
PARTIAL_MATCH_WEIGHT_RATIO = {
    "canonical_name": 0.7,
    "canonical_phone": 0.8,
    "canonical_address": 0.6,
}
NICKNAME_GROUPS = (
    frozenset({"JOHN", "JON", "JOHNNY", "JONATHAN"}),
    frozenset({"KATE", "KATIE", "KATHERINE", "KATHY"}),
    frozenset({"MIKE", "MICHAEL", "MICKEY"}),
    frozenset({"BOB", "BOBBY", "ROB", "ROBBIE", "ROBERT"}),
)
NICKNAME_INDEX = {
    alias: index
    for index, group in enumerate(NICKNAME_GROUPS)
    for alias in group
}


def _matching_non_empty_field(
    left: dict[str, str],
    right: dict[str, str],
    field_name: str,
) -> bool:
    left_value = left.get(field_name, "").strip()
    right_value = right.get(field_name, "").strip()
    return bool(left_value and right_value and left_value == right_value)


def _name_tokens(value: str) -> tuple[str, ...]:
    return tuple(token for token in value.strip().upper().split() if token)


def _nickname_group(value: str) -> int | None:
    return NICKNAME_INDEX.get(value.strip().upper())


def _is_partial_name_match(left_value: str, right_value: str) -> bool:
    left_tokens = _name_tokens(left_value)
    right_tokens = _name_tokens(right_value)
    if len(left_tokens) < 2 or len(right_tokens) < 2:
        return False

    if left_tokens[-1] != right_tokens[-1]:
        return False

    left_first = left_tokens[0]
    right_first = right_tokens[0]
    if left_first == right_first:
        return True
    if left_first[:1] and right_first[:1] and left_first[0] == right_first[0]:
        return True

    left_group = _nickname_group(left_first)
    right_group = _nickname_group(right_first)
    return left_group is not None and left_group == right_group


def _partial_field_match_weight(
    left: dict[str, str],
    right: dict[str, str],
    field_name: str,
    weight: float,
) -> tuple[str, float] | None:
    left_value = left.get(field_name, "").strip()
    right_value = right.get(field_name, "").strip()
    if not left_value or not right_value:
        return None

    is_partial_match = False
    if field_name == "canonical_name":
        is_partial_match = _is_partial_name_match(left_value, right_value)
    elif field_name == "canonical_phone":
        is_partial_match = _is_partial_phone_match(left_value, right_value)
    elif field_name == "canonical_address":
        is_partial_match = _is_partial_address_match(left_value, right_value)

    if not is_partial_match:
        return None

    partial_weight = round(weight * PARTIAL_MATCH_WEIGHT_RATIO[field_name], 4)
    if partial_weight <= 0.0:
        return None
    return (f"{field_name}_partial", partial_weight)


def _phone_digits(value: str) -> str:
    return re.sub(r"\D+", "", value)


def _is_partial_phone_match(left_value: str, right_value: str) -> bool:
    left_digits = _phone_digits(left_value)
    right_digits = _phone_digits(right_value)
    if len(left_digits) < 10 or len(right_digits) < 10:
        return False
    return left_digits[-10:] == right_digits[-10:]


_ADDRESS_DIRECTION_TOKENS = frozenset(
    {"NORTH", "SOUTH", "EAST", "WEST", "NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST"}
)
_ADDRESS_SUFFIX_TOKENS = frozenset(
    {
        "AVENUE",
        "BOULEVARD",
        "CIRCLE",
        "COURT",
        "DRIVE",
        "HIGHWAY",
        "LANE",
        "PARKWAY",
        "PLACE",
        "ROAD",
        "STREET",
        "TERRACE",
    }
)
_ADDRESS_STOP_TOKENS = _ADDRESS_DIRECTION_TOKENS | _ADDRESS_SUFFIX_TOKENS | {"UNIT", "PO", "BOX"}


def _address_signature(value: str) -> tuple[str, frozenset[str]]:
    tokens = tuple(token for token in value.strip().upper().split() if token)
    if not tokens:
        return "", frozenset()

    house_number = next((token for token in tokens if any(character.isdigit() for character in token)), "")
    if "UNIT" in tokens:
        tokens = tokens[: tokens.index("UNIT")]

    street_tokens = frozenset(token for token in tokens if token != house_number and token not in _ADDRESS_STOP_TOKENS)
    return house_number, street_tokens


def _is_partial_address_match(left_value: str, right_value: str) -> bool:
    left_house_number, left_street_tokens = _address_signature(left_value)
    right_house_number, right_street_tokens = _address_signature(right_value)
    if not left_house_number or left_house_number != right_house_number:
        return False
    if not left_street_tokens or not right_street_tokens:
        return False
    return bool(left_street_tokens & right_street_tokens)


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
            continue

        partial_match = _partial_field_match_weight(left, right, field_name, weight)
        if partial_match is not None:
            partial_field_name, partial_weight = partial_match
            score += partial_weight
            matched_fields.append(partial_field_name)
            reason_trace.append(f"{partial_field_name}:{partial_weight:g}")

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

