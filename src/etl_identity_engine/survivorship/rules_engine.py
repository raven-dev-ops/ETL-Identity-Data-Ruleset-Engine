"""Deterministic survivorship helpers."""

from __future__ import annotations

from typing import Sequence

from etl_identity_engine.survivorship.provenance import build_provenance, flatten_provenance


def _choose_item(
    values: list[dict[str, str]],
    source_priority: Sequence[str] | None = None,
) -> dict[str, str]:
    priority = list(source_priority or [])
    priority_rank = {name: idx for idx, name in enumerate(priority)}

    def sort_key(item: dict[str, str]) -> tuple[int, str, str]:
        source = item.get("source_system", "")
        rank = priority_rank.get(source, len(priority_rank))
        ts = item.get("updated_at", "")
        return (-rank, ts, item.get("value", ""))

    non_empty = [item for item in values if item.get("value")]
    if not non_empty:
        return {}

    return max(non_empty, key=sort_key)


def choose_value(values: list[dict[str, str]], source_priority: Sequence[str] | None = None) -> str:
    return _choose_item(values, source_priority=source_priority).get("value", "")


def _group_records(
    records: list[dict[str, str]],
    *,
    allow_person_entity_fallback: bool = False,
) -> list[list[dict[str, str]]]:
    grouping_field = None
    grouping_fields = ["cluster_id"]
    if allow_person_entity_fallback:
        grouping_fields.append("person_entity_id")

    for field_name in grouping_fields:
        if any(str(row.get(field_name, "")).strip() for row in records):
            grouping_field = field_name
            break

    if grouping_field is None:
        return [[row] for row in sorted(records, key=lambda row: row.get("source_record_id", ""))]

    grouped: dict[str, list[dict[str, str]]] = {}
    groups: list[list[dict[str, str]]] = []
    for row in records:
        group_id = str(row.get(grouping_field, "")).strip()
        if not group_id:
            groups.append([row])
            continue
        grouped.setdefault(group_id, []).append(row)

    groups.extend(grouped[group_id] for group_id in sorted(grouped))
    return groups


def merge_records(
    records: list[dict[str, str]],
    *,
    golden_id: str = "G-00001",
    source_priority: Sequence[str] | None = None,
    field_rules: dict[str, str] | None = None,
) -> dict[str, str]:
    if not records:
        return {}

    preferred_sources = list(source_priority or ("source_a", "source_b"))
    configured_field_rules = field_rules or {}

    def _choose(field: str) -> tuple[str, dict[str, str]]:
        values = []
        for row in records:
            values.append(
                {
                    "value": row.get(field, ""),
                    "source_system": row.get("source_system", ""),
                    "updated_at": row.get("updated_at", ""),
                    "source_record_id": row.get("source_record_id", ""),
                }
            )
        chosen = _choose_item(values, source_priority=preferred_sources)
        rule_name = configured_field_rules.get(field, "source_priority_then_non_null")
        provenance = build_provenance(
            field,
            chosen.get("source_record_id", ""),
            rule_name,
            source_system=chosen.get("source_system", ""),
        )
        return chosen.get("value", ""), provenance

    golden_record = {"golden_id": golden_id}
    for field_name in ("first_name", "last_name", "dob", "address", "phone"):
        chosen_value, provenance = _choose(field_name)
        golden_record[field_name] = chosen_value
        golden_record.update(flatten_provenance(field_name, provenance))
    person_entity_id = next(
        (str(row.get("person_entity_id", "")).strip() for row in records if row.get("person_entity_id")),
        "",
    )
    cluster_id = next(
        (str(row.get("cluster_id", "")).strip() for row in records if row.get("cluster_id")),
        "",
    )
    if person_entity_id:
        golden_record["person_entity_id"] = person_entity_id
    if cluster_id:
        golden_record["cluster_id"] = cluster_id
    golden_record["source_record_count"] = str(len(records))
    return golden_record


def build_golden_records(
    records: list[dict[str, str]],
    *,
    source_priority: Sequence[str] | None = None,
    field_rules: dict[str, str] | None = None,
    allow_person_entity_fallback: bool = False,
) -> list[dict[str, str]]:
    grouped_records = _group_records(
        records,
        allow_person_entity_fallback=allow_person_entity_fallback,
    )
    return [
        merge_records(
            group,
            golden_id=f"G-{index:05d}",
            source_priority=source_priority,
            field_rules=field_rules,
        )
        for index, group in enumerate(grouped_records, start=1)
    ]

