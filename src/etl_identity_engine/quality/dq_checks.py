"""Data quality checks for scaffold runs."""

from __future__ import annotations

from collections import defaultdict


def summarize_missing_fields(rows: list[dict[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    if not rows:
        return {}

    field_names = list(rows[0].keys())
    for row in rows:
        for field in field_names:
            if not str(row.get(field, "")).strip():
                counts[field] += 1
    return dict(counts)

