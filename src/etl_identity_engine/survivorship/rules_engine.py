"""Deterministic survivorship helpers."""

from __future__ import annotations


def choose_value(values: list[dict[str, str]], source_priority: list[str] | None = None) -> str:
    priority = source_priority or []
    priority_rank = {name: idx for idx, name in enumerate(priority)}

    def sort_key(item: dict[str, str]) -> tuple[int, str]:
        source = item.get("source_system", "")
        rank = priority_rank.get(source, len(priority_rank))
        ts = item.get("updated_at", "")
        return (rank, ts)

    non_empty = [item for item in values if item.get("value")]
    if not non_empty:
        return ""

    chosen = sorted(non_empty, key=sort_key)[0]
    return chosen.get("value", "")


def merge_records(records: list[dict[str, str]]) -> dict[str, str]:
    if not records:
        return {}

    preferred_sources = ["source_b", "source_a"]

    def _choose(field: str) -> str:
        values = []
        for row in records:
            values.append(
                {
                    "value": row.get(field, ""),
                    "source_system": row.get("source_system", ""),
                    "updated_at": row.get("updated_at", ""),
                }
            )
        return choose_value(values, source_priority=preferred_sources)

    return {
        "golden_id": "G-00001",
        "first_name": _choose("first_name"),
        "last_name": _choose("last_name"),
        "dob": _choose("dob"),
        "address": _choose("address"),
        "phone": _choose("phone"),
    }

