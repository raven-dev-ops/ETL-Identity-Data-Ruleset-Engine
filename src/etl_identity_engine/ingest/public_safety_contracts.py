"""Versioned CAD/RMS public-safety source bundle contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml

from etl_identity_engine.generate.synth_generator import (
    INCIDENT_HEADERS,
    INCIDENT_LINK_HEADERS,
    PERSON_HEADERS,
)
from etl_identity_engine.ingest.public_safety_mapping import (
    PublicSafetyMappingOverlay,
    PublicSafetyMappingOverlayError,
    apply_public_safety_mapping_overlay,
    build_public_safety_mapping_diff_report,
    load_public_safety_mapping_overlay,
)
from etl_identity_engine.ingest.public_safety_vendor_profiles import (
    PublicSafetyVendorProfileError,
    load_packaged_public_safety_mapping_overlay,
)
from etl_identity_engine.io.read import read_dict_fieldnames, read_dict_rows


PUBLIC_SAFETY_CONTRACT_MARKER = "contract_manifest.yml"
PUBLIC_SAFETY_CONTRACT_FORMATS = frozenset({".csv", ".parquet"})
CAD_CALL_FOR_SERVICE_CONTRACT_NAME = "cad_call_for_service"
RMS_REPORT_PERSON_CONTRACT_NAME = "rms_report_person"
PUBLIC_SAFETY_CONTRACT_VERSION_V1 = "v1"


class PublicSafetyContractValidationError(ValueError):
    """Raised when a CAD or RMS source bundle is incomplete or inconsistent."""


@dataclass(frozen=True)
class PublicSafetyContractFileSpec:
    logical_name: str
    default_filename: str
    required_columns: tuple[str, ...]


@dataclass(frozen=True)
class PublicSafetyContractSpec:
    contract_name: str
    contract_version: str
    source_system: str
    file_specs: tuple[PublicSafetyContractFileSpec, ...]

    @property
    def file_spec_by_name(self) -> dict[str, PublicSafetyContractFileSpec]:
        return {spec.logical_name: spec for spec in self.file_specs}


@dataclass(frozen=True)
class ValidatedPublicSafetyContractFile:
    logical_name: str
    path: Path
    format: str
    source_fieldnames: tuple[str, ...]
    fieldnames: tuple[str, ...]
    row_count: int
    rows: tuple[dict[str, str], ...]
    diff_report: dict[str, object]


@dataclass(frozen=True)
class ValidatedPublicSafetyContractBundle:
    bundle_dir: Path
    marker_path: Path
    contract_name: str
    contract_version: str
    source_system: str
    mapping_overlay_path: Path | None
    mapping_overlay_relative_path: str | None
    vendor_profile: str | None
    files: tuple[ValidatedPublicSafetyContractFile, ...]

    def to_summary(self) -> dict[str, object]:
        return {
            "bundle_dir": str(self.bundle_dir),
            "marker_path": str(self.marker_path),
            "contract_name": self.contract_name,
            "contract_version": self.contract_version,
            "source_system": self.source_system,
            "mapping_overlay_path": None if self.mapping_overlay_path is None else str(self.mapping_overlay_path),
            "mapping_overlay_relative_path": self.mapping_overlay_relative_path,
            "vendor_profile": self.vendor_profile,
            "files": {
                file.logical_name: {
                    "path": str(file.path),
                    "relative_path": str(file.path.relative_to(self.bundle_dir)).replace("\\", "/"),
                    "format": file.format,
                    "row_count": file.row_count,
                    "source_fieldnames": list(file.source_fieldnames),
                    "fieldnames": list(file.fieldnames),
                    "diff_report": file.diff_report,
                }
                for file in self.files
            },
        }


CAD_CALL_FOR_SERVICE_CONTRACT = PublicSafetyContractSpec(
    contract_name=CAD_CALL_FOR_SERVICE_CONTRACT_NAME,
    contract_version=PUBLIC_SAFETY_CONTRACT_VERSION_V1,
    source_system="cad",
    file_specs=(
        PublicSafetyContractFileSpec(
            logical_name="person_records",
            default_filename="cad_person_records.csv",
            required_columns=PERSON_HEADERS,
        ),
        PublicSafetyContractFileSpec(
            logical_name="incident_records",
            default_filename="cad_incident_records.csv",
            required_columns=INCIDENT_HEADERS,
        ),
        PublicSafetyContractFileSpec(
            logical_name="incident_person_links",
            default_filename="cad_incident_person_links.csv",
            required_columns=INCIDENT_LINK_HEADERS,
        ),
    ),
)

RMS_REPORT_PERSON_CONTRACT = PublicSafetyContractSpec(
    contract_name=RMS_REPORT_PERSON_CONTRACT_NAME,
    contract_version=PUBLIC_SAFETY_CONTRACT_VERSION_V1,
    source_system="rms",
    file_specs=(
        PublicSafetyContractFileSpec(
            logical_name="person_records",
            default_filename="rms_person_records.csv",
            required_columns=PERSON_HEADERS,
        ),
        PublicSafetyContractFileSpec(
            logical_name="incident_records",
            default_filename="rms_incident_records.csv",
            required_columns=INCIDENT_HEADERS,
        ),
        PublicSafetyContractFileSpec(
            logical_name="incident_person_links",
            default_filename="rms_incident_person_links.csv",
            required_columns=INCIDENT_LINK_HEADERS,
        ),
    ),
)

SUPPORTED_PUBLIC_SAFETY_CONTRACTS = {
    CAD_CALL_FOR_SERVICE_CONTRACT.contract_name: CAD_CALL_FOR_SERVICE_CONTRACT,
    RMS_REPORT_PERSON_CONTRACT.contract_name: RMS_REPORT_PERSON_CONTRACT,
}


def _bundle_error(bundle_dir: Path, message: str) -> PublicSafetyContractValidationError:
    return PublicSafetyContractValidationError(f"{bundle_dir.name}: {message}")


def _load_marker_mapping(marker_path: Path) -> Mapping[str, object]:
    if not marker_path.exists():
        raise PublicSafetyContractValidationError(
            f"{marker_path.parent.name}: missing {PUBLIC_SAFETY_CONTRACT_MARKER}"
        )

    data = yaml.safe_load(marker_path.read_text(encoding="utf-8"))
    if not isinstance(data, Mapping):
        raise PublicSafetyContractValidationError(
            f"{marker_path.parent.name}: {PUBLIC_SAFETY_CONTRACT_MARKER} must contain a mapping"
        )
    return data


def _require_non_empty_string(
    mapping: Mapping[str, object],
    key: str,
    *,
    bundle_dir: Path,
    context: str,
) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _bundle_error(bundle_dir, f"{context}.{key} must be a non-empty string")
    return value.strip()


def _require_string_mapping(
    mapping: Mapping[str, object],
    key: str,
    *,
    bundle_dir: Path,
    context: str,
) -> dict[str, str]:
    value = mapping.get(key)
    if not isinstance(value, Mapping):
        raise _bundle_error(bundle_dir, f"{context}.{key} must be a mapping")

    resolved: dict[str, str] = {}
    for item_key, item_value in value.items():
        if not isinstance(item_key, str) or not item_key.strip():
            raise _bundle_error(bundle_dir, f"{context}.{key} contains a non-string key")
        if not isinstance(item_value, str) or not item_value.strip():
            raise _bundle_error(
                bundle_dir,
                f"{context}.{key}.{item_key} must be a non-empty string",
            )
        resolved[item_key.strip()] = item_value.strip()
    return resolved


def _optional_non_empty_string(
    mapping: Mapping[str, object],
    key: str,
    *,
    bundle_dir: Path,
    context: str,
) -> str | None:
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise _bundle_error(bundle_dir, f"{context}.{key} must be a non-empty string")
    return value.strip()


def _resolve_bundle_relative_path(bundle_dir: Path, relative_path: str, *, label: str) -> Path:
    candidate = Path(relative_path)
    if candidate.is_absolute():
        raise _bundle_error(bundle_dir, f"{label} paths must be relative: {relative_path}")

    resolved = (bundle_dir / candidate).resolve()
    bundle_root = bundle_dir.resolve()
    try:
        resolved.relative_to(bundle_root)
    except ValueError as exc:
        raise _bundle_error(
            bundle_dir,
            f"{label} path escapes the bundle root: {relative_path}",
        ) from exc
    return resolved


def _resolve_bundle_file_path(bundle_dir: Path, relative_path: str) -> Path:
    return _resolve_bundle_relative_path(bundle_dir, relative_path, label="contract file")


def _validate_required_file_keys(
    bundle_dir: Path,
    spec: PublicSafetyContractSpec,
    files: Mapping[str, str],
) -> None:
    expected = set(spec.file_spec_by_name)
    actual = set(files)
    missing = sorted(expected - actual)
    if missing:
        raise _bundle_error(
            bundle_dir,
            f"contract files mapping is missing required entries: {', '.join(missing)}",
        )
    unexpected = sorted(actual - expected)
    if unexpected:
        raise _bundle_error(
            bundle_dir,
            f"contract files mapping contains unsupported entries: {', '.join(unexpected)}",
        )


def _allowed_fields_by_file(
    spec: PublicSafetyContractSpec,
) -> dict[str, tuple[str, ...]]:
    return {
        file_spec.logical_name: file_spec.required_columns
        for file_spec in spec.file_specs
    }


def _load_mapping_overlay(
    bundle_dir: Path,
    *,
    marker: Mapping[str, object],
    spec: PublicSafetyContractSpec,
    explicit_mapping_overlay_path: Path | None,
    explicit_vendor_profile: str | None,
) -> PublicSafetyMappingOverlay | None:
    marker_overlay_reference = _optional_non_empty_string(
        marker,
        "mapping_overlay",
        bundle_dir=bundle_dir,
        context="contract manifest",
    )
    marker_vendor_profile = _optional_non_empty_string(
        marker,
        "vendor_profile",
        bundle_dir=bundle_dir,
        context="contract manifest",
    )
    if marker_overlay_reference is not None and marker_vendor_profile is not None:
        raise _bundle_error(
            bundle_dir,
            "contract manifest cannot define both mapping_overlay and vendor_profile",
        )
    if explicit_mapping_overlay_path is not None and explicit_vendor_profile is not None:
        raise _bundle_error(
            bundle_dir,
            "explicit mapping overlay and vendor_profile cannot both be provided",
        )
    if explicit_mapping_overlay_path is not None and marker_vendor_profile is not None:
        raise _bundle_error(
            bundle_dir,
            "vendor_profile from the contract manifest cannot be combined with an explicit mapping overlay",
        )
    if explicit_vendor_profile is not None and marker_overlay_reference is not None:
        raise _bundle_error(
            bundle_dir,
            "mapping_overlay from the contract manifest cannot be combined with an explicit vendor_profile",
        )

    resolved_overlay_path = explicit_mapping_overlay_path
    if resolved_overlay_path is None and marker_overlay_reference is not None:
        resolved_overlay_path = _resolve_bundle_relative_path(
            bundle_dir,
            marker_overlay_reference,
            label="mapping overlay",
        )
    resolved_vendor_profile = explicit_vendor_profile or marker_vendor_profile
    if resolved_overlay_path is not None:
        try:
            return load_public_safety_mapping_overlay(
                resolved_overlay_path,
                contract_name=spec.contract_name,
                contract_version=spec.contract_version,
                allowed_fields_by_file=_allowed_fields_by_file(spec),
            )
        except PublicSafetyMappingOverlayError as exc:
            raise _bundle_error(bundle_dir, f"mapping overlay is invalid: {exc}") from exc
    if resolved_vendor_profile is not None:
        try:
            return load_packaged_public_safety_mapping_overlay(
                resolved_vendor_profile,
                contract_name=spec.contract_name,
                contract_version=spec.contract_version,
                allowed_fields_by_file=_allowed_fields_by_file(spec),
            )
        except PublicSafetyVendorProfileError as exc:
            raise _bundle_error(bundle_dir, str(exc)) from exc
    return None


def _load_and_canonicalize_rows(
    bundle_dir: Path,
    logical_name: str,
    path: Path,
    required_columns: tuple[str, ...],
    *,
    overlay: PublicSafetyMappingOverlay | None,
) -> tuple[tuple[str, ...], tuple[str, ...], list[dict[str, str]], dict[str, object]]:
    try:
        source_fieldnames = read_dict_fieldnames(path)
        source_rows = read_dict_rows(path)
    except FileNotFoundError as exc:
        raise _bundle_error(
            bundle_dir,
            f"contract file '{logical_name}' not found: {path.relative_to(bundle_dir)}",
        ) from exc

    diff_report = build_public_safety_mapping_diff_report(
        logical_name=logical_name,
        source_fieldnames=source_fieldnames,
        required_columns=required_columns,
        overlay=overlay,
    )

    if overlay is None:
        missing_columns = [column for column in required_columns if column not in source_fieldnames]
        if missing_columns:
            raise _bundle_error(
                bundle_dir,
                f"contract file '{logical_name}' is missing required columns: "
                f"{', '.join(missing_columns)}",
            )

    try:
        rows = apply_public_safety_mapping_overlay(
            logical_name=logical_name,
            rows=source_rows,
            source_fieldnames=source_fieldnames,
            required_columns=required_columns,
            overlay=overlay,
        )
    except PublicSafetyMappingOverlayError as exc:
        raise _bundle_error(bundle_dir, f"contract file '{logical_name}' failed field mapping: {exc}") from exc
    return source_fieldnames, required_columns, rows, diff_report


def _inspect_contract_file(
    bundle_dir: Path,
    *,
    file_spec: PublicSafetyContractFileSpec,
    relative_path: str | None,
    overlay: PublicSafetyMappingOverlay | None,
) -> dict[str, object]:
    summary: dict[str, object] = {
        "logical_name": file_spec.logical_name,
    }
    if relative_path is None:
        summary["status"] = "failed"
        summary["validation_error"] = (
            "contract files mapping is missing this required entry"
        )
        return summary

    summary["relative_path"] = relative_path
    try:
        resolved_path = _resolve_bundle_file_path(bundle_dir, relative_path)
    except PublicSafetyContractValidationError as exc:
        summary["status"] = "failed"
        summary["validation_error"] = str(exc)
        return summary

    summary["path"] = str(resolved_path)
    summary["format"] = resolved_path.suffix.lower().lstrip(".")
    if resolved_path.suffix.lower() not in PUBLIC_SAFETY_CONTRACT_FORMATS:
        summary["status"] = "failed"
        summary["validation_error"] = (
            f"contract file '{file_spec.logical_name}' must be CSV or Parquet: {relative_path}"
        )
        return summary

    if not resolved_path.exists():
        summary["status"] = "failed"
        summary["validation_error"] = (
            f"contract file '{file_spec.logical_name}' not found: {relative_path}"
        )
        return summary

    try:
        source_fieldnames = read_dict_fieldnames(resolved_path)
        source_rows = read_dict_rows(resolved_path)
    except FileNotFoundError as exc:
        summary["status"] = "failed"
        summary["validation_error"] = str(exc)
        return summary

    diff_report = build_public_safety_mapping_diff_report(
        logical_name=file_spec.logical_name,
        source_fieldnames=source_fieldnames,
        required_columns=file_spec.required_columns,
        overlay=overlay,
    )
    summary.update(
        {
            "status": (
                "passed"
                if not diff_report["missing_required_canonical_fields"]
                else "failed"
            ),
            "row_count": len(source_rows),
            "source_fieldnames": list(source_fieldnames),
            "fieldnames": list(file_spec.required_columns),
            "diff_report": diff_report,
        }
    )
    if diff_report["missing_required_canonical_fields"]:
        summary["validation_error"] = (
            "missing canonical mappings for required fields: "
            + ", ".join(diff_report["missing_required_canonical_fields"])
        )
    return summary


def inspect_public_safety_contract_bundle(
    bundle_dir: Path,
    *,
    mapping_overlay_path: Path | None = None,
    vendor_profile: str | None = None,
) -> dict[str, object]:
    """Inspect a public-safety contract bundle and return JSON-safe results."""
    resolved_bundle_dir = bundle_dir.resolve()
    summary: dict[str, object] = {
        "bundle_dir": str(resolved_bundle_dir),
        "marker_path": str((resolved_bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER).resolve()),
    }
    try:
        validated = validate_public_safety_contract_bundle(
            resolved_bundle_dir,
            mapping_overlay_path=mapping_overlay_path,
            vendor_profile=vendor_profile,
        )
    except (FileNotFoundError, NotADirectoryError, PublicSafetyContractValidationError) as exc:
        summary["status"] = "failed"
        summary["validation_error"] = str(exc)
    else:
        return {
            "status": "passed",
            **validated.to_summary(),
        }

    if not resolved_bundle_dir.exists():
        return summary
    if not resolved_bundle_dir.is_dir():
        summary["validation_error"] = f"Public-safety bundle must be a directory: {resolved_bundle_dir}"
        return summary

    marker_path = resolved_bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER
    try:
        marker = _load_marker_mapping(marker_path)
    except PublicSafetyContractValidationError:
        return summary

    contract_name = marker.get("contract_name")
    if isinstance(contract_name, str) and contract_name.strip():
        summary["contract_name"] = contract_name.strip()
    contract_version = marker.get("contract_version")
    if isinstance(contract_version, str) and contract_version.strip():
        summary["contract_version"] = contract_version.strip()

    files_value = marker.get("files")
    file_mapping: dict[str, str] = {}
    if isinstance(files_value, Mapping):
        for key, value in files_value.items():
            if isinstance(key, str) and key.strip() and isinstance(value, str) and value.strip():
                file_mapping[key.strip()] = value.strip()

    spec = None
    if isinstance(contract_name, str):
        spec = SUPPORTED_PUBLIC_SAFETY_CONTRACTS.get(contract_name.strip())
    if spec is None:
        return summary

    summary["source_system"] = spec.source_system
    overlay_error: str | None = None
    try:
        resolved_mapping_overlay_path = None if mapping_overlay_path is None else mapping_overlay_path.resolve()
        overlay = _load_mapping_overlay(
            resolved_bundle_dir,
            marker=marker,
            spec=spec,
            explicit_mapping_overlay_path=resolved_mapping_overlay_path,
            explicit_vendor_profile=vendor_profile,
        )
    except PublicSafetyContractValidationError as exc:
        overlay = None
        overlay_error = str(exc)

    mapping_overlay_path_value = None
    mapping_overlay_relative_path = None
    vendor_profile_value = vendor_profile
    if overlay is not None:
        vendor_profile_value = overlay.vendor_profile
        if overlay.vendor_profile is None:
            mapping_overlay_path_value = str(overlay.overlay_path)
            try:
                mapping_overlay_relative_path = str(
                    overlay.overlay_path.relative_to(resolved_bundle_dir)
                ).replace("\\", "/")
            except ValueError:
                mapping_overlay_relative_path = None
    summary["mapping_overlay_path"] = mapping_overlay_path_value
    summary["mapping_overlay_relative_path"] = mapping_overlay_relative_path
    summary["vendor_profile"] = vendor_profile_value
    if overlay_error is not None:
        summary["mapping_overlay_error"] = overlay_error

    file_summaries = {
        file_spec.logical_name: _inspect_contract_file(
            resolved_bundle_dir,
            file_spec=file_spec,
            relative_path=file_mapping.get(file_spec.logical_name),
            overlay=overlay,
        )
        for file_spec in spec.file_specs
    }
    summary["files"] = file_summaries
    return summary


def _require_non_empty_values(
    bundle_dir: Path,
    rows: list[dict[str, str]],
    *,
    dataset_name: str,
    field_name: str,
) -> None:
    blank_rows = sum(1 for row in rows if not row.get(field_name, "").strip())
    if blank_rows:
        raise _bundle_error(
            bundle_dir,
            f"{dataset_name} contains {blank_rows} row(s) with blank {field_name}",
        )


def _validate_unique_values(
    bundle_dir: Path,
    rows: list[dict[str, str]],
    *,
    dataset_name: str,
    field_name: str,
) -> None:
    _require_non_empty_values(bundle_dir, rows, dataset_name=dataset_name, field_name=field_name)
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        value = row.get(field_name, "").strip()
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    if duplicates:
        raise _bundle_error(
            bundle_dir,
            f"{dataset_name} contains duplicate {field_name} values: {', '.join(sorted(duplicates))}",
        )


def _validate_source_system_values(
    bundle_dir: Path,
    rows: list[dict[str, str]],
    *,
    dataset_name: str,
    expected_source_system: str,
) -> None:
    _require_non_empty_values(bundle_dir, rows, dataset_name=dataset_name, field_name="source_system")
    mismatched = sorted(
        {
            row.get("source_system", "").strip()
            for row in rows
            if row.get("source_system", "").strip() != expected_source_system
        }
    )
    if mismatched:
        raise _bundle_error(
            bundle_dir,
            f"{dataset_name} contains source_system values that do not match the contract: "
            f"{', '.join(mismatched)}",
        )


def _validate_link_references(
    bundle_dir: Path,
    *,
    person_rows: list[dict[str, str]],
    incident_rows: list[dict[str, str]],
    link_rows: list[dict[str, str]],
) -> None:
    person_by_source_record = {
        row.get("source_record_id", "").strip(): row.get("person_entity_id", "").strip()
        for row in person_rows
    }
    incident_ids = {row.get("incident_id", "").strip() for row in incident_rows}

    missing_incident_ids: set[str] = set()
    missing_source_record_ids: set[str] = set()
    mismatched_person_entities: set[str] = set()

    for row in link_rows:
        incident_id = row.get("incident_id", "").strip()
        source_record_id = row.get("source_record_id", "").strip()
        person_entity_id = row.get("person_entity_id", "").strip()

        if incident_id not in incident_ids:
            missing_incident_ids.add(incident_id)
        if source_record_id not in person_by_source_record:
            missing_source_record_ids.add(source_record_id)
            continue
        if person_entity_id != person_by_source_record[source_record_id]:
            mismatched_person_entities.add(
                row.get("incident_person_link_id", "").strip() or source_record_id
            )

    if missing_incident_ids:
        raise _bundle_error(
            bundle_dir,
            "incident_person_links references unknown incident_id values: "
            + ", ".join(sorted(missing_incident_ids)),
        )
    if missing_source_record_ids:
        raise _bundle_error(
            bundle_dir,
            "incident_person_links references unknown source_record_id values: "
            + ", ".join(sorted(missing_source_record_ids)),
        )
    if mismatched_person_entities:
        raise _bundle_error(
            bundle_dir,
            "incident_person_links contains person_entity_id values that do not match the "
            "referenced source record for link ids: "
            + ", ".join(sorted(mismatched_person_entities)),
        )


def validate_public_safety_contract_bundle(
    bundle_dir: Path,
    *,
    mapping_overlay_path: Path | None = None,
    vendor_profile: str | None = None,
) -> ValidatedPublicSafetyContractBundle:
    """Validate a versioned CAD or RMS source bundle and return its summary."""
    if not bundle_dir.exists():
        raise FileNotFoundError(f"Public-safety bundle not found: {bundle_dir}")
    if not bundle_dir.is_dir():
        raise NotADirectoryError(f"Public-safety bundle must be a directory: {bundle_dir}")

    marker_path = bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER
    marker = _load_marker_mapping(marker_path)

    unexpected_keys = sorted(
        set(marker) - {"contract_name", "contract_version", "files", "mapping_overlay", "vendor_profile"}
    )
    if unexpected_keys:
        raise _bundle_error(
            bundle_dir,
            f"{PUBLIC_SAFETY_CONTRACT_MARKER} contains unsupported keys: {', '.join(unexpected_keys)}",
        )

    contract_name = _require_non_empty_string(
        marker,
        "contract_name",
        bundle_dir=bundle_dir,
        context="contract manifest",
    )
    contract_version = _require_non_empty_string(
        marker,
        "contract_version",
        bundle_dir=bundle_dir,
        context="contract manifest",
    )
    file_mapping = _require_string_mapping(
        marker,
        "files",
        bundle_dir=bundle_dir,
        context="contract manifest",
    )

    spec = SUPPORTED_PUBLIC_SAFETY_CONTRACTS.get(contract_name)
    if spec is None:
        raise _bundle_error(bundle_dir, f"unsupported contract_name: {contract_name}")
    if contract_version != spec.contract_version:
        raise _bundle_error(
            bundle_dir,
            f"unsupported {contract_name} contract_version: {contract_version}",
        )

    resolved_mapping_overlay_path = None if mapping_overlay_path is None else mapping_overlay_path.resolve()
    overlay = _load_mapping_overlay(
        bundle_dir,
        marker=marker,
        spec=spec,
        explicit_mapping_overlay_path=resolved_mapping_overlay_path,
        explicit_vendor_profile=vendor_profile,
    )

    _validate_required_file_keys(bundle_dir, spec, file_mapping)

    validated_files: list[ValidatedPublicSafetyContractFile] = []
    rows_by_logical_name: dict[str, list[dict[str, str]]] = {}

    for file_spec in spec.file_specs:
        relative_path = file_mapping[file_spec.logical_name]
        resolved_path = _resolve_bundle_file_path(bundle_dir, relative_path)
        if resolved_path.suffix.lower() not in PUBLIC_SAFETY_CONTRACT_FORMATS:
            raise _bundle_error(
                bundle_dir,
                f"contract file '{file_spec.logical_name}' must be CSV or Parquet: {relative_path}",
            )
        source_fieldnames, fieldnames, rows, diff_report = _load_and_canonicalize_rows(
            bundle_dir,
            file_spec.logical_name,
            resolved_path,
            file_spec.required_columns,
            overlay=overlay,
        )
        validated_files.append(
            ValidatedPublicSafetyContractFile(
                logical_name=file_spec.logical_name,
                path=resolved_path,
                format=resolved_path.suffix.lower().lstrip("."),
                source_fieldnames=source_fieldnames,
                fieldnames=fieldnames,
                row_count=len(rows),
                rows=tuple(dict(row) for row in rows),
                diff_report=diff_report,
            )
        )
        rows_by_logical_name[file_spec.logical_name] = rows

    person_rows = rows_by_logical_name["person_records"]
    incident_rows = rows_by_logical_name["incident_records"]
    link_rows = rows_by_logical_name["incident_person_links"]

    _validate_unique_values(
        bundle_dir,
        person_rows,
        dataset_name="person_records",
        field_name="source_record_id",
    )
    _validate_unique_values(
        bundle_dir,
        incident_rows,
        dataset_name="incident_records",
        field_name="incident_id",
    )
    _validate_unique_values(
        bundle_dir,
        link_rows,
        dataset_name="incident_person_links",
        field_name="incident_person_link_id",
    )

    _require_non_empty_values(
        bundle_dir,
        person_rows,
        dataset_name="person_records",
        field_name="person_entity_id",
    )
    _require_non_empty_values(
        bundle_dir,
        incident_rows,
        dataset_name="incident_records",
        field_name="incident_id",
    )
    _require_non_empty_values(
        bundle_dir,
        link_rows,
        dataset_name="incident_person_links",
        field_name="incident_id",
    )
    _require_non_empty_values(
        bundle_dir,
        link_rows,
        dataset_name="incident_person_links",
        field_name="source_record_id",
    )
    _require_non_empty_values(
        bundle_dir,
        link_rows,
        dataset_name="incident_person_links",
        field_name="person_entity_id",
    )

    _validate_source_system_values(
        bundle_dir,
        person_rows,
        dataset_name="person_records",
        expected_source_system=spec.source_system,
    )
    _validate_source_system_values(
        bundle_dir,
        incident_rows,
        dataset_name="incident_records",
        expected_source_system=spec.source_system,
    )
    _validate_link_references(
        bundle_dir,
        person_rows=person_rows,
        incident_rows=incident_rows,
        link_rows=link_rows,
    )

    mapping_overlay_relative_path = None
    if overlay is not None:
        try:
            mapping_overlay_relative_path = str(
                overlay.overlay_path.relative_to(bundle_dir.resolve())
            ).replace("\\", "/")
        except ValueError:
            mapping_overlay_relative_path = None

    return ValidatedPublicSafetyContractBundle(
        bundle_dir=bundle_dir.resolve(),
        marker_path=marker_path.resolve(),
        contract_name=contract_name,
        contract_version=contract_version,
        source_system=spec.source_system,
        mapping_overlay_path=(
            None if overlay is None or overlay.vendor_profile is not None else overlay.overlay_path
        ),
        mapping_overlay_relative_path=mapping_overlay_relative_path,
        vendor_profile=None if overlay is None else overlay.vendor_profile,
        files=tuple(validated_files),
    )
