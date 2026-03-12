"""Phone normalization helpers."""

from __future__ import annotations

import re


def normalize_phone(value: str) -> str:
    return re.sub(r"\D+", "", value)

