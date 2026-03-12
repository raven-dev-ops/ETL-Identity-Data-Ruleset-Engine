"""Date normalization helpers."""

from __future__ import annotations

from datetime import datetime


_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
)


def normalize_date(value: str) -> str | None:
    raw = value.strip()
    if not raw:
        return None

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

