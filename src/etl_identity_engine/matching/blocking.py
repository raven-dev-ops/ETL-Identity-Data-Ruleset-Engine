"""Candidate generation using simple blocking keys."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
from typing import Sequence


DEFAULT_BLOCKING_PASSES: tuple[tuple[str, ...], ...] = (("last_initial", "dob"),)


@dataclass(frozen=True)
class BlockingPassMetric:
    pass_name: str
    fields: tuple[str, ...]
    raw_candidate_pair_count: int
    new_candidate_pair_count: int
    cumulative_candidate_pair_count: int


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
    pairs, _ = generate_candidates_with_metrics(records, blocking_passes=blocking_passes)
    return pairs


def generate_candidates_with_metrics(
    records: list[dict[str, str]],
    *,
    blocking_passes: Sequence[Sequence[str]] | None = None,
    pass_names: Sequence[str] | None = None,
) -> tuple[list[tuple[dict[str, str], dict[str, str]]], list[BlockingPassMetric]]:
    configured_passes = tuple(tuple(fields) for fields in (blocking_passes or DEFAULT_BLOCKING_PASSES))
    if pass_names is not None and len(pass_names) != len(configured_passes):
        raise ValueError("pass_names must match the number of blocking passes")
    effective_pass_names = tuple(
        pass_names or ("+".join(fields) for fields in configured_passes)
    )

    seen_pairs: set[tuple[int, int]] = set()
    pairs: list[tuple[dict[str, str], dict[str, str]]] = []
    metrics: list[BlockingPassMetric] = []
    indexed_records = list(enumerate(records))

    for pass_name, fields in zip(effective_pass_names, configured_passes, strict=True):
        buckets: dict[tuple[str, ...], list[tuple[int, dict[str, str]]]] = defaultdict(list)
        for index, row in indexed_records:
            key = blocking_key(row, fields=fields)
            if not all(key):
                continue
            buckets[key].append((index, row))

        raw_candidate_pair_count = 0
        new_candidate_pair_count = 0
        for bucket_rows in buckets.values():
            for (left_index, left), (right_index, right) in combinations(bucket_rows, 2):
                raw_candidate_pair_count += 1
                pair_key = (left_index, right_index)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                new_candidate_pair_count += 1
                pairs.append((left, right))

        metrics.append(
            BlockingPassMetric(
                pass_name=str(pass_name),
                fields=fields,
                raw_candidate_pair_count=raw_candidate_pair_count,
                new_candidate_pair_count=new_candidate_pair_count,
                cumulative_candidate_pair_count=len(pairs),
            )
        )
    return pairs, metrics
