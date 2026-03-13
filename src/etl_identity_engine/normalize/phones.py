"""Phone normalization helpers."""

from __future__ import annotations

import re


def normalize_phone(value: str, *, digits_only: bool = True) -> str:
    if digits_only:
        return re.sub(r"\D+", "", value)
    return value.strip()

