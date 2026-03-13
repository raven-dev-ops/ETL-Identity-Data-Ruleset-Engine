"""Provenance helpers for chosen golden attributes."""

from __future__ import annotations


def build_provenance(
    field_name: str,
    source_record_id: str,
    rule_name: str,
    *,
    source_system: str = "",
) -> dict[str, str]:
    provenance = {
        "field_name": field_name,
        "source_record_id": source_record_id,
        "rule_name": rule_name,
    }
    if source_system:
        provenance["source_system"] = source_system
    return provenance


def flatten_provenance(field_name: str, provenance: dict[str, str]) -> dict[str, str]:
    return {
        f"{field_name}_source_record_id": provenance.get("source_record_id", ""),
        f"{field_name}_source_system": provenance.get("source_system", ""),
        f"{field_name}_rule_name": provenance.get("rule_name", ""),
    }

