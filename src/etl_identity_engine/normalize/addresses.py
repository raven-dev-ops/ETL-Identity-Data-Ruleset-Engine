"""Address normalization helpers."""

from __future__ import annotations

import re

_DIRECTION_MAP = {
    "N": "NORTH",
    "S": "SOUTH",
    "E": "EAST",
    "W": "WEST",
    "NE": "NORTHEAST",
    "NW": "NORTHWEST",
    "SE": "SOUTHEAST",
    "SW": "SOUTHWEST",
}
_SUFFIX_MAP = {
    "AVE": "AVENUE",
    "AV": "AVENUE",
    "BLVD": "BOULEVARD",
    "CIR": "CIRCLE",
    "CT": "COURT",
    "DR": "DRIVE",
    "HWY": "HIGHWAY",
    "LN": "LANE",
    "PKWY": "PARKWAY",
    "PL": "PLACE",
    "RD": "ROAD",
    "ST": "STREET",
    "TER": "TERRACE",
}
_UNIT_MARKER_MAP = {
    "APARTMENT": "UNIT",
    "APT": "UNIT",
    "RM": "UNIT",
    "ROOM": "UNIT",
    "STE": "UNIT",
    "SUITE": "UNIT",
    "UNIT": "UNIT",
}
_KNOWN_UNIT_MARKERS = frozenset(_UNIT_MARKER_MAP.values()) | frozenset(_UNIT_MARKER_MAP)


def normalize_address(value: str) -> str:
    text = value.strip().upper()
    if not text:
        return ""

    text = re.sub(r"\bP\s*\.?\s*O\s*\.?\b", "PO", text)
    text = text.replace("#", " UNIT ")
    tokens = re.findall(r"[A-Z0-9]+", text)
    if not tokens:
        return ""

    if len(tokens) >= 2 and tokens[0] == "PO" and tokens[1] == "BOX":
        return " ".join(tokens).strip()

    unit_tokens: list[str] = []
    tokens_without_unit: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        mapped_unit = _UNIT_MARKER_MAP.get(token)
        if mapped_unit and not unit_tokens:
            unit_tokens.append(mapped_unit)
            index += 1
            while index < len(tokens) and tokens[index] in _KNOWN_UNIT_MARKERS:
                index += 1
            if index < len(tokens):
                unit_tokens.append(tokens[index])
                index += 1
            continue
        tokens_without_unit.append(token)
        index += 1

    house_number = ""
    remaining_tokens: list[str] = []
    for token in tokens_without_unit:
        if not house_number and any(character.isdigit() for character in token):
            house_number = token
            continue
        remaining_tokens.append(token)

    normalized_tokens = [
        _SUFFIX_MAP.get(_DIRECTION_MAP.get(token, token), _DIRECTION_MAP.get(token, token))
        for token in remaining_tokens
    ]
    ordered_tokens = ([house_number] if house_number else []) + normalized_tokens + unit_tokens
    return " ".join(token for token in ordered_tokens if token).strip()

