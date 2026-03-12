"""Schema models used by synthetic data generation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PersonSourceRecord:
    source_record_id: str
    person_entity_id: str
    source_system: str
    first_name: str
    last_name: str
    dob: str
    address: str
    city: str
    state: str
    postal_code: str
    phone: str
    updated_at: str
    is_conflict_variant: str
    conflict_types: str


@dataclass(frozen=True)
class IncidentRecord:
    incident_id: str
    source_system: str
    occurred_at: str
    location: str
    city: str
    state: str


@dataclass(frozen=True)
class IncidentPersonLink:
    incident_person_link_id: str
    incident_id: str
    person_entity_id: str
    source_record_id: str
    role: str


@dataclass(frozen=True)
class AddressHistoryRecord:
    address_history_id: str
    person_entity_id: str
    address: str
    city: str
    state: str
    postal_code: str
    effective_start: str
    effective_end: str
    is_current: str

