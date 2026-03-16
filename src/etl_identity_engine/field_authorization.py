"""Field-level authorization helpers for service and delivery surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from etl_identity_engine.output_contracts import (
    CROSSWALK_HEADERS,
    GOLDEN_HEADERS,
    PUBLIC_SAFETY_GOLDEN_ACTIVITY_HEADERS,
    PUBLIC_SAFETY_INCIDENT_IDENTITY_HEADERS,
)


FieldAuthorizationAction = Literal["allow", "mask", "deny"]

SERVICE_GOLDEN_RECORD_SURFACE = "service.golden_record"
SERVICE_CROSSWALK_LOOKUP_SURFACE = "service.crosswalk_lookup"
SERVICE_PUBLIC_SAFETY_GOLDEN_ACTIVITY_SURFACE = "service.public_safety_golden_activity"
SERVICE_PUBLIC_SAFETY_INCIDENT_IDENTITY_SURFACE = "service.public_safety_incident_identity"
DELIVERY_GOLDEN_RECORDS_SURFACE = "delivery.golden_records"
DELIVERY_SOURCE_TO_GOLDEN_CROSSWALK_SURFACE = "delivery.source_to_golden_crosswalk"

SUPPORTED_FIELD_AUTHORIZATION_ACTIONS = frozenset({"allow", "mask", "deny"})
MASKED_FIELD_VALUE = "[MASKED]"
SUPPORTED_FIELD_AUTHORIZATION_SURFACES = {
    SERVICE_GOLDEN_RECORD_SURFACE: GOLDEN_HEADERS,
    SERVICE_CROSSWALK_LOOKUP_SURFACE: CROSSWALK_HEADERS,
    SERVICE_PUBLIC_SAFETY_GOLDEN_ACTIVITY_SURFACE: PUBLIC_SAFETY_GOLDEN_ACTIVITY_HEADERS,
    SERVICE_PUBLIC_SAFETY_INCIDENT_IDENTITY_SURFACE: PUBLIC_SAFETY_INCIDENT_IDENTITY_HEADERS,
    DELIVERY_GOLDEN_RECORDS_SURFACE: GOLDEN_HEADERS,
    DELIVERY_SOURCE_TO_GOLDEN_CROSSWALK_SURFACE: CROSSWALK_HEADERS,
}


@dataclass(frozen=True)
class FieldAuthorizationConfig:
    surface_rules: dict[str, dict[str, FieldAuthorizationAction]]


class FieldAuthorizationError(ValueError):
    """Raised when field-authorization evaluation fails closed."""


class FieldAuthorizationDenied(FieldAuthorizationError):
    """Raised when a surface is configured to deny one or more fields."""

    def __init__(self, *, surface: str, denied_fields: tuple[str, ...]) -> None:
        self.surface = surface
        self.denied_fields = denied_fields
        formatted_fields = ", ".join(denied_fields)
        super().__init__(
            f"Field-authorization policy blocks access to {surface} for fields: {formatted_fields}"
        )


def _rules_for_surface(
    surface: str,
    config: FieldAuthorizationConfig | None,
) -> dict[str, FieldAuthorizationAction]:
    if surface not in SUPPORTED_FIELD_AUTHORIZATION_SURFACES:
        raise FieldAuthorizationError(
            f"Unsupported field-authorization surface {surface!r}; "
            f"expected one of {sorted(SUPPORTED_FIELD_AUTHORIZATION_SURFACES)}"
        )
    if config is None:
        return {}
    return dict(config.surface_rules.get(surface, {}))


def ensure_surface_allowed(
    *,
    surface: str,
    config: FieldAuthorizationConfig | None,
) -> None:
    denied_fields = tuple(
        sorted(
            field_name
            for field_name, action in _rules_for_surface(surface, config).items()
            if action == "deny"
        )
    )
    if denied_fields:
        raise FieldAuthorizationDenied(surface=surface, denied_fields=denied_fields)


def _mask_value(value: Any) -> Any:
    if value in (None, ""):
        return value
    if isinstance(value, str):
        return MASKED_FIELD_VALUE
    raise FieldAuthorizationError(
        "Field-authorization masking currently supports string-valued fields only"
    )


def apply_field_authorization_to_mapping(
    payload: Mapping[str, Any],
    *,
    surface: str,
    config: FieldAuthorizationConfig | None,
) -> dict[str, Any]:
    rules = _rules_for_surface(surface, config)
    ensure_surface_allowed(surface=surface, config=config)
    authorized = dict(payload)
    for field_name, action in rules.items():
        if field_name not in authorized:
            raise FieldAuthorizationError(
                f"Field-authorization surface {surface!r} cannot evaluate unknown field {field_name!r}"
            )
        if action == "mask":
            authorized[field_name] = _mask_value(authorized[field_name])
    return authorized


def apply_field_authorization_to_rows(
    rows: list[Mapping[str, Any]],
    *,
    surface: str,
    config: FieldAuthorizationConfig | None,
) -> list[dict[str, Any]]:
    ensure_surface_allowed(surface=surface, config=config)
    return [
        apply_field_authorization_to_mapping(row, surface=surface, config=config)
        for row in rows
    ]
