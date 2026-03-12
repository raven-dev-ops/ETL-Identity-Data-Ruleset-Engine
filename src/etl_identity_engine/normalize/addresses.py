"""Address normalization helpers."""

from __future__ import annotations

import re

_SUFFIX_MAP = {
    "ST": "STREET",
    "ST.": "STREET",
    "AVE": "AVENUE",
    "AVE.": "AVENUE",
    "RD": "ROAD",
    "RD.": "ROAD",
    "LN": "LANE",
    "LN.": "LANE",
    "DR": "DRIVE",
    "DR.": "DRIVE",
}


def normalize_address(value: str) -> str:
    text = re.sub(r"\s+", " ", value.strip().upper())
    parts = text.split(" ")
    normalized = [_SUFFIX_MAP.get(part, part) for part in parts]
    return " ".join(normalized).strip()

