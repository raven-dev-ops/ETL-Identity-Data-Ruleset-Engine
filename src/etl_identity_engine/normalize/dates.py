"""Date normalization helpers."""

from __future__ import annotations

from datetime import datetime


_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
)


def normalize_date(
    value: str,
    *,
    accepted_formats: tuple[str, ...] | None = None,
    output_format: str = "%Y-%m-%d",
) -> str | None:
    raw = value.strip()
    if not raw:
        return None

    for fmt in accepted_formats or _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime(output_format)
        except ValueError:
            continue
    return None

