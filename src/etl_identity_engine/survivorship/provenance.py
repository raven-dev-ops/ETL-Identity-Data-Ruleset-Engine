"""Provenance helpers for chosen golden attributes."""

from __future__ import annotations


def build_provenance(field_name: str, source_record_id: str, rule_name: str) -> dict[str, str]:
    return {
        "field_name": field_name,
        "source_record_id": source_record_id,
        "rule_name": rule_name,
    }

