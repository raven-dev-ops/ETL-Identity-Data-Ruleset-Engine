"""Candidate generation using simple blocking keys."""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from typing import Sequence


DEFAULT_BLOCKING_PASSES: tuple[tuple[str, ...], ...] = (("last_initial", "dob"),)


def _blocking_value(record: dict[str, str], field_name: str) -> str:
    if field_name == "last_initial":
        last_name = record.get("last_name", "").strip().upper()
        return last_name[:1]
    if field_name == "last_name":
        return record.get("last_name", "").strip().upper()
    if field_name == "dob":
        return (record.get("canonical_dob") or record.get("dob", "")).strip()
    if field_name == "birth_year":
        dob = (record.get("canonical_dob") or record.get("dob", "")).strip()
        return dob[:4] if len(dob) >= 4 else ""
    return str(record.get(field_name, "")).strip()


def blocking_key(
    record: dict[str, str],
    *,
    fields: Sequence[str] = DEFAULT_BLOCKING_PASSES[0],
) -> tuple[str, ...]:
    return tuple(_blocking_value(record, field_name) for field_name in fields)


def generate_candidates(
    records: list[dict[str, str]],
    *,
    blocking_passes: Sequence[Sequence[str]] | None = None,
) -> list[tuple[dict[str, str], dict[str, str]]]:
    configured_passes = tuple(tuple(fields) for fields in (blocking_passes or DEFAULT_BLOCKING_PASSES))
    seen_pairs: set[tuple[int, int]] = set()
    pairs: list[tuple[dict[str, str], dict[str, str]]] = []
    indexed_records = list(enumerate(records))

    for fields in configured_passes:
        buckets: dict[tuple[str, ...], list[tuple[int, dict[str, str]]]] = defaultdict(list)
        for index, row in indexed_records:
            key = blocking_key(row, fields=fields)
            if not all(key):
                continue
            buckets[key].append((index, row))

        for bucket_rows in buckets.values():
            for (left_index, left), (right_index, right) in combinations(bucket_rows, 2):
                pair_key = (left_index, right_index)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                pairs.append((left, right))
    return pairs
