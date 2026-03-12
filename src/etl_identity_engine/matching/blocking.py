"""Candidate generation using simple blocking keys."""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations


def blocking_key(record: dict[str, str]) -> tuple[str, str]:
    last = record.get("last_name", "").strip().upper()
    dob = record.get("canonical_dob") or record.get("dob", "")
    return (last[:1], dob)


def generate_candidates(
    records: list[dict[str, str]],
) -> list[tuple[dict[str, str], dict[str, str]]]:
    buckets: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in records:
        buckets[blocking_key(row)].append(row)

    pairs: list[tuple[dict[str, str], dict[str, str]]] = []
    for bucket_rows in buckets.values():
        for left, right in combinations(bucket_rows, 2):
            pairs.append((left, right))
    return pairs
