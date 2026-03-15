"""Vendor-specific field-mapping overlays for CAD/RMS source bundles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml


PUBLIC_SAFETY_MAPPING_OVERLAY_VERSION = "v1"


class PublicSafetyMappingOverlayError(ValueError):
    """Raised when a public-safety mapping overlay is incomplete or inconsistent."""


@dataclass(frozen=True)
class PublicSafetyMappingFileOverlay:
    logical_name: str
    column_map: dict[str, str]
    defaults: dict[str, str]


@dataclass(frozen=True)
class PublicSafetyMappingOverlay:
    overlay_path: Path
    overlay_label: str
    contract_name: str
    contract_version: str
    vendor_profile: str | None
    files: dict[str, PublicSafetyMappingFileOverlay]


def _overlay_error(overlay_path: Path, message: str) -> PublicSafetyMappingOverlayError:
    return PublicSafetyMappingOverlayError(f"{overlay_path.name}: {message}")


def _require_non_empty_string(
    mapping: Mapping[str, object],
    key: str,
    *,
    overlay_path: Path,
    context: str,
) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _overlay_error(overlay_path, f"{context}.{key} must be a non-empty string")
    return value.strip()


def _validate_allowed_keys(
    mapping: Mapping[str, object],
    *,
    allowed_keys: set[str],
    overlay_path: Path,
    context: str,
) -> None:
    unexpected_keys = sorted(set(mapping) - allowed_keys)
    if unexpected_keys:
        raise _overlay_error(
            overlay_path,
            f"{context} contains unsupported keys: {', '.join(unexpected_keys)}",
        )


def _load_overlay_mapping(overlay_path: Path) -> Mapping[str, object]:
    if not overlay_path.exists():
        raise FileNotFoundError(f"Public-safety mapping overlay not found: {overlay_path}")

    data = yaml.safe_load(overlay_path.read_text(encoding="utf-8"))
    if not isinstance(data, Mapping):
        raise _overlay_error(overlay_path, "overlay must contain a mapping")
    return data


def _load_file_overlay(
    raw_file_overlay: Mapping[str, object],
    *,
    overlay_path: Path,
    logical_name: str,
    allowed_fields: tuple[str, ...],
) -> PublicSafetyMappingFileOverlay:
    context = f"files.{logical_name}"
    _validate_allowed_keys(
        raw_file_overlay,
        allowed_keys={"columns", "defaults"},
        overlay_path=overlay_path,
        context=context,
    )

    raw_columns = raw_file_overlay.get("columns", {})
    if not isinstance(raw_columns, Mapping):
        raise _overlay_error(overlay_path, f"{context}.columns must be a mapping")

    raw_defaults = raw_file_overlay.get("defaults", {})
    if not isinstance(raw_defaults, Mapping):
        raise _overlay_error(overlay_path, f"{context}.defaults must be a mapping")

    column_map: dict[str, str] = {}
    defaults: dict[str, str] = {}
    allowed_field_set = set(allowed_fields)

    for canonical_field, source_field in raw_columns.items():
        if not isinstance(canonical_field, str) or not canonical_field.strip():
            raise _overlay_error(
                overlay_path,
                f"{context}.columns contains a non-string canonical field",
            )
        if canonical_field.strip() not in allowed_field_set:
            raise _overlay_error(
                overlay_path,
                f"{context}.columns.{canonical_field} is not a supported canonical field",
            )
        if not isinstance(source_field, str) or not source_field.strip():
            raise _overlay_error(
                overlay_path,
                f"{context}.columns.{canonical_field} must be a non-empty string",
            )
        column_map[canonical_field.strip()] = source_field.strip()

    for canonical_field, value in raw_defaults.items():
        if not isinstance(canonical_field, str) or not canonical_field.strip():
            raise _overlay_error(
                overlay_path,
                f"{context}.defaults contains a non-string canonical field",
            )
        if canonical_field.strip() not in allowed_field_set:
            raise _overlay_error(
                overlay_path,
                f"{context}.defaults.{canonical_field} is not a supported canonical field",
            )
        if canonical_field.strip() in column_map:
            raise _overlay_error(
                overlay_path,
                f"{context}.{canonical_field.strip()} cannot be defined in both columns and defaults",
            )
        if isinstance(value, bool | int | float | str):  # type: ignore[arg-type]
            defaults[canonical_field.strip()] = str(value)
            continue
        raise _overlay_error(
            overlay_path,
            f"{context}.defaults.{canonical_field} must be a scalar value",
        )

    if not column_map and not defaults:
        raise _overlay_error(
            overlay_path,
            f"{context} must define at least one column mapping or default",
        )

    return PublicSafetyMappingFileOverlay(
        logical_name=logical_name,
        column_map=column_map,
        defaults=defaults,
    )


def load_public_safety_mapping_overlay(
    overlay_path: Path,
    *,
    contract_name: str,
    contract_version: str,
    allowed_fields_by_file: Mapping[str, tuple[str, ...]],
) -> PublicSafetyMappingOverlay:
    """Load and validate a vendor-column overlay for a CAD/RMS contract bundle."""
    resolved_path = overlay_path.resolve()
    overlay = _load_overlay_mapping(resolved_path)

    _validate_allowed_keys(
        overlay,
        allowed_keys={"overlay_version", "contract_name", "contract_version", "files"},
        overlay_path=resolved_path,
        context="overlay",
    )

    overlay_version = _require_non_empty_string(
        overlay,
        "overlay_version",
        overlay_path=resolved_path,
        context="overlay",
    )
    if overlay_version != PUBLIC_SAFETY_MAPPING_OVERLAY_VERSION:
        raise _overlay_error(
            resolved_path,
            f"overlay.overlay_version must be {PUBLIC_SAFETY_MAPPING_OVERLAY_VERSION!r}",
        )

    overlay_contract_name = _require_non_empty_string(
        overlay,
        "contract_name",
        overlay_path=resolved_path,
        context="overlay",
    )
    if overlay_contract_name != contract_name:
        raise _overlay_error(
            resolved_path,
            f"overlay.contract_name must be {contract_name!r}",
        )

    overlay_contract_version = _require_non_empty_string(
        overlay,
        "contract_version",
        overlay_path=resolved_path,
        context="overlay",
    )
    if overlay_contract_version != contract_version:
        raise _overlay_error(
            resolved_path,
            f"overlay.contract_version must be {contract_version!r}",
        )

    raw_files = overlay.get("files")
    if not isinstance(raw_files, Mapping) or not raw_files:
        raise _overlay_error(
            resolved_path,
            "overlay.files must be a non-empty mapping",
        )

    allowed_file_names = set(allowed_fields_by_file)
    resolved_files: dict[str, PublicSafetyMappingFileOverlay] = {}
    for logical_name, raw_file_overlay in raw_files.items():
        if not isinstance(logical_name, str) or not logical_name.strip():
            raise _overlay_error(
                resolved_path,
                "overlay.files contains a non-string logical file name",
            )
        normalized_name = logical_name.strip()
        if normalized_name not in allowed_file_names:
            raise _overlay_error(
                resolved_path,
                f"overlay.files.{normalized_name} is not a supported logical file name",
            )
        if not isinstance(raw_file_overlay, Mapping):
            raise _overlay_error(
                resolved_path,
                f"overlay.files.{normalized_name} must be a mapping",
            )
        resolved_files[normalized_name] = _load_file_overlay(
            raw_file_overlay,
            overlay_path=resolved_path,
            logical_name=normalized_name,
            allowed_fields=allowed_fields_by_file[normalized_name],
        )

    return PublicSafetyMappingOverlay(
        overlay_path=resolved_path,
        overlay_label=resolved_path.name,
        contract_name=overlay_contract_name,
        contract_version=overlay_contract_version,
        vendor_profile=None,
        files=resolved_files,
    )


def apply_public_safety_mapping_overlay(
    *,
    logical_name: str,
    rows: list[dict[str, str]],
    source_fieldnames: tuple[str, ...],
    required_columns: tuple[str, ...],
    overlay: PublicSafetyMappingOverlay | None,
) -> list[dict[str, str]]:
    """Transform vendor columns into the canonical contract shape."""
    file_overlay = None if overlay is None else overlay.files.get(logical_name)
    column_map = {} if file_overlay is None else file_overlay.column_map
    defaults = {} if file_overlay is None else file_overlay.defaults

    missing_source_columns = sorted(
        {
            source_column
            for source_column in column_map.values()
            if source_column not in source_fieldnames
        }
    )
    if missing_source_columns:
        overlay_name = "<implicit canonical passthrough>" if overlay is None else overlay.overlay_label
        raise PublicSafetyMappingOverlayError(
            f"{overlay_name}: files.{logical_name} references missing source columns: "
            + ", ".join(missing_source_columns)
        )

    missing_canonical_fields = [
        field
        for field in required_columns
        if field not in column_map and field not in source_fieldnames and field not in defaults
    ]
    if missing_canonical_fields:
        overlay_name = "<implicit canonical passthrough>" if overlay is None else overlay.overlay_label
        raise PublicSafetyMappingOverlayError(
            f"{overlay_name}: files.{logical_name} does not provide canonical fields: "
            + ", ".join(missing_canonical_fields)
        )

    transformed_rows: list[dict[str, str]] = []
    for row in rows:
        transformed_row: dict[str, str] = {}
        for field in required_columns:
            if field in column_map:
                transformed_row[field] = str(row.get(column_map[field], ""))
            elif field in row:
                transformed_row[field] = str(row.get(field, ""))
            else:
                transformed_row[field] = defaults[field]
        transformed_rows.append(transformed_row)
    return transformed_rows
