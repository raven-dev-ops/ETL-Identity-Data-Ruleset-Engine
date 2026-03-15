"""Synthetic vendor-shaped CAD/RMS onboarding bundle generation."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil

import yaml

from etl_identity_engine.generate.synth_generator import (
    INCIDENT_HEADERS,
    INCIDENT_LINK_HEADERS,
    PERSON_HEADERS,
    generate_synthetic_sources,
)
from etl_identity_engine.ingest.public_safety_contracts import (
    CAD_CALL_FOR_SERVICE_CONTRACT,
    PUBLIC_SAFETY_CONTRACT_MARKER,
    RMS_REPORT_PERSON_CONTRACT,
    PublicSafetyContractSpec,
)
from etl_identity_engine.ingest.public_safety_vendor_profiles import (
    PublicSafetyVendorProfile,
    get_public_safety_vendor_profile,
    list_public_safety_vendor_profiles,
    load_packaged_public_safety_mapping_overlay,
)
from etl_identity_engine.io.read import read_dict_rows
from etl_identity_engine.io.write import write_csv_dicts


SOURCE_SYSTEM_ORDER = ("cad", "rms")


@dataclass(frozen=True)
class PublicSafetyVendorBatchGenerationResult:
    output_dir: Path
    manifest_path: Path
    summary_path: Path
    summary: dict[str, object]


def _allowed_fields_by_file(spec: PublicSafetyContractSpec) -> dict[str, tuple[str, ...]]:
    return {
        "person_records": PERSON_HEADERS,
        "incident_records": INCIDENT_HEADERS,
        "incident_person_links": INCIDENT_LINK_HEADERS,
    }


def _bundle_contract_for_source_system(source_system: str) -> PublicSafetyContractSpec:
    if source_system == "cad":
        return CAD_CALL_FOR_SERVICE_CONTRACT
    if source_system == "rms":
        return RMS_REPORT_PERSON_CONTRACT
    raise ValueError(f"unsupported source_system: {source_system}")


def _select_vendor_profiles(
    *,
    cad_profiles: tuple[str, ...],
    rms_profiles: tuple[str, ...],
) -> tuple[PublicSafetyVendorProfile, ...]:
    selected: list[PublicSafetyVendorProfile] = []
    seen: set[str] = set()

    requested_by_system = {
        "cad": cad_profiles or tuple(profile.name for profile in list_public_safety_vendor_profiles(source_system="cad")),
        "rms": rms_profiles or tuple(profile.name for profile in list_public_safety_vendor_profiles(source_system="rms")),
    }

    for source_system in SOURCE_SYSTEM_ORDER:
        for profile_name in requested_by_system[source_system]:
            profile = get_public_safety_vendor_profile(profile_name)
            if profile.source_system != source_system:
                raise ValueError(
                    f"vendor profile {profile_name!r} is for source_system {profile.source_system!r}, "
                    f"not {source_system!r}"
                )
            if profile.name in seen:
                continue
            selected.append(profile)
            seen.add(profile.name)
    return tuple(selected)


def _load_seed_rows(seed_dir: Path) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    return (
        read_dict_rows(seed_dir / "person_source_a.csv"),
        read_dict_rows(seed_dir / "person_source_b.csv"),
        read_dict_rows(seed_dir / "incident_records.csv"),
        read_dict_rows(seed_dir / "incident_person_links.csv"),
    )


def _build_person_lookup(
    source_a_rows: list[dict[str, str]],
    source_b_rows: list[dict[str, str]],
) -> dict[str, dict[str, str]]:
    preferred_rows: dict[str, dict[str, str]] = {}
    ordered_rows = sorted(
        source_a_rows + source_b_rows,
        key=lambda row: (
            str(row.get("person_entity_id", "")),
            0 if str(row.get("source_system", "")) == "source_a" else 1,
            str(row.get("source_record_id", "")),
        ),
    )
    for row in ordered_rows:
        person_entity_id = str(row.get("person_entity_id", "")).strip()
        if not person_entity_id or person_entity_id in preferred_rows:
            continue
        preferred_rows[person_entity_id] = dict(row)
    return preferred_rows


def _assign_incidents_to_source_systems(
    incident_rows: list[dict[str, str]],
    requested_source_systems: tuple[str, ...],
) -> dict[str, list[dict[str, str]]]:
    assignments = {source_system: [] for source_system in requested_source_systems}
    if not requested_source_systems:
        return assignments

    sorted_rows = [dict(row) for row in sorted(incident_rows, key=lambda row: str(row.get("incident_id", "")))]
    if len(requested_source_systems) == 1:
        source_system = requested_source_systems[0]
        assignments[source_system] = [{**row, "source_system": source_system} for row in sorted_rows]
        return assignments

    observed = {
        source_system: [dict(row) for row in sorted_rows if str(row.get("source_system", "")).strip() == source_system]
        for source_system in requested_source_systems
    }
    if all(observed[source_system] for source_system in requested_source_systems):
        return observed

    for index, row in enumerate(sorted_rows):
        source_system = requested_source_systems[index % len(requested_source_systems)]
        assignments[source_system].append({**row, "source_system": source_system})
    return assignments


def _build_canonical_bundle_rows(
    *,
    source_system: str,
    incident_rows: list[dict[str, str]],
    incident_link_rows: list[dict[str, str]],
    person_lookup: dict[str, dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    relevant_incident_ids = {
        str(row.get("incident_id", "")).strip()
        for row in incident_rows
        if str(row.get("incident_id", "")).strip()
    }
    relevant_link_rows = [
        dict(row)
        for row in sorted(incident_link_rows, key=lambda row: str(row.get("incident_person_link_id", "")))
        if str(row.get("incident_id", "")).strip() in relevant_incident_ids
    ]

    person_entity_ids = sorted(
        {
            str(row.get("person_entity_id", "")).strip()
            for row in relevant_link_rows
            if str(row.get("person_entity_id", "")).strip()
        }
    )

    person_rows: list[dict[str, str]] = []
    source_record_ids_by_entity: dict[str, str] = {}
    for index, person_entity_id in enumerate(person_entity_ids, start=1):
        person_row = person_lookup.get(person_entity_id)
        if person_row is None:
            continue
        source_record_id = f"{source_system.upper()}-PERSON-{index:06d}"
        source_record_ids_by_entity[person_entity_id] = source_record_id
        person_rows.append(
            {
                "source_record_id": source_record_id,
                "person_entity_id": person_entity_id,
                "source_system": source_system,
                "first_name": str(person_row.get("first_name", "")),
                "last_name": str(person_row.get("last_name", "")),
                "dob": str(person_row.get("dob", "")),
                "address": str(person_row.get("address", "")),
                "city": str(person_row.get("city", "")),
                "state": str(person_row.get("state", "")),
                "postal_code": str(person_row.get("postal_code", "")),
                "phone": str(person_row.get("phone", "")),
                "updated_at": str(person_row.get("updated_at", "")),
                "is_conflict_variant": str(person_row.get("is_conflict_variant", "false")),
                "conflict_types": str(person_row.get("conflict_types", "")),
            }
        )

    incident_rows_out: list[dict[str, str]] = []
    incident_id_map: dict[str, str] = {}
    for index, incident_row in enumerate(
        sorted(incident_rows, key=lambda row: str(row.get("incident_id", ""))),
        start=1,
    ):
        original_incident_id = str(incident_row.get("incident_id", "")).strip()
        synthetic_incident_id = f"{source_system.upper()}-INC-{index:06d}"
        incident_id_map[original_incident_id] = synthetic_incident_id
        incident_rows_out.append(
            {
                "incident_id": synthetic_incident_id,
                "source_system": source_system,
                "occurred_at": str(incident_row.get("occurred_at", "")),
                "location": str(incident_row.get("location", "")),
                "city": str(incident_row.get("city", "")),
                "state": str(incident_row.get("state", "")),
            }
        )

    incident_person_link_rows: list[dict[str, str]] = []
    link_index = 1
    for link_row in relevant_link_rows:
        person_entity_id = str(link_row.get("person_entity_id", "")).strip()
        mapped_source_record_id = source_record_ids_by_entity.get(person_entity_id)
        mapped_incident_id = incident_id_map.get(str(link_row.get("incident_id", "")).strip())
        if mapped_source_record_id is None or mapped_incident_id is None:
            continue
        incident_person_link_rows.append(
            {
                "incident_person_link_id": f"{source_system.upper()}-LINK-{link_index:07d}",
                "incident_id": mapped_incident_id,
                "person_entity_id": person_entity_id,
                "source_record_id": mapped_source_record_id,
                "role": str(link_row.get("role", "")),
            }
        )
        link_index += 1

    return {
        "person_records": person_rows,
        "incident_records": incident_rows_out,
        "incident_person_links": incident_person_link_rows,
    }


def _vendor_rows_from_canonical_rows(
    *,
    logical_name: str,
    canonical_rows: list[dict[str, str]],
    required_columns: tuple[str, ...],
    profile: PublicSafetyVendorProfile,
) -> tuple[list[dict[str, str]], tuple[str, ...]]:
    overlay = load_packaged_public_safety_mapping_overlay(
        profile.name,
        contract_name=profile.contract_name,
        contract_version=profile.contract_version,
        allowed_fields_by_file=_allowed_fields_by_file(_bundle_contract_for_source_system(profile.source_system)),
    )
    file_overlay = overlay.files[logical_name]

    fieldnames: list[str] = []
    for canonical_field in required_columns:
        source_column = file_overlay.column_map.get(canonical_field)
        if source_column is None:
            if canonical_field in file_overlay.defaults:
                continue
            source_column = canonical_field
        if source_column not in fieldnames:
            fieldnames.append(source_column)

    vendor_rows: list[dict[str, str]] = []
    for row in canonical_rows:
        vendor_row: dict[str, str] = {}
        for canonical_field in required_columns:
            source_column = file_overlay.column_map.get(canonical_field)
            if source_column is not None:
                vendor_row[source_column] = str(row.get(canonical_field, ""))
                continue
            if canonical_field in file_overlay.defaults:
                continue
            vendor_row[canonical_field] = str(row.get(canonical_field, ""))
        vendor_rows.append(vendor_row)
    return vendor_rows, tuple(fieldnames)


def _bundle_file_name(logical_name: str) -> str:
    return f"vendor_{logical_name}.csv"


def _bundle_directory_name(profile: PublicSafetyVendorProfile) -> str:
    return profile.name


def generate_public_safety_vendor_batches(
    output_dir: Path,
    *,
    profile: str = "small",
    seed: int = 42,
    person_count_override: int | None = None,
    cad_profiles: tuple[str, ...] = (),
    rms_profiles: tuple[str, ...] = (),
) -> PublicSafetyVendorBatchGenerationResult:
    """Generate vendor-shaped synthetic CAD/RMS bundles for onboarding rehearsal."""
    resolved_output_dir = output_dir.resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    selected_profiles = _select_vendor_profiles(
        cad_profiles=cad_profiles,
        rms_profiles=rms_profiles,
    )
    if not selected_profiles:
        raise ValueError("at least one CAD or RMS vendor profile must be selected")

    seed_dir = resolved_output_dir / "seed_data"
    landing_dir = resolved_output_dir / "landing"
    bundles_dir = resolved_output_dir / "bundles"
    manifest_path = resolved_output_dir / "synthetic_vendor_manifest.yml"
    summary_path = resolved_output_dir / "public_safety_vendor_batch_summary.json"

    generate_synthetic_sources(
        seed_dir,
        profile=profile,
        seed=seed,
        formats=("csv",),
        person_count_override=person_count_override,
    )

    source_a_rows, source_b_rows, incident_rows, incident_link_rows = _load_seed_rows(seed_dir)
    person_lookup = _build_person_lookup(source_a_rows, source_b_rows)

    landing_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(seed_dir / "person_source_a.csv", landing_dir / "source_a.csv")
    shutil.copy2(seed_dir / "person_source_b.csv", landing_dir / "source_b.csv")

    requested_source_systems = tuple(
        source_system
        for source_system in SOURCE_SYSTEM_ORDER
        if any(profile.source_system == source_system for profile in selected_profiles)
    )
    incidents_by_source_system = _assign_incidents_to_source_systems(
        incident_rows,
        requested_source_systems,
    )
    canonical_rows_by_source_system = {
        source_system: _build_canonical_bundle_rows(
            source_system=source_system,
            incident_rows=incidents_by_source_system[source_system],
            incident_link_rows=incident_link_rows,
            person_lookup=person_lookup,
        )
        for source_system in requested_source_systems
    }

    bundle_summaries: list[dict[str, object]] = []
    manifest_source_bundles: list[dict[str, object]] = []

    for vendor_profile in selected_profiles:
        contract = _bundle_contract_for_source_system(vendor_profile.source_system)
        canonical_rows = canonical_rows_by_source_system[vendor_profile.source_system]
        bundle_dir = bundles_dir / _bundle_directory_name(vendor_profile)
        bundle_dir.mkdir(parents=True, exist_ok=True)

        file_summaries: list[dict[str, object]] = []
        for file_spec in contract.file_specs:
            vendor_rows, fieldnames = _vendor_rows_from_canonical_rows(
                logical_name=file_spec.logical_name,
                canonical_rows=canonical_rows[file_spec.logical_name],
                required_columns=file_spec.required_columns,
                profile=vendor_profile,
            )
            file_name = _bundle_file_name(file_spec.logical_name)
            file_path = bundle_dir / file_name
            write_csv_dicts(file_path, vendor_rows, fieldnames=fieldnames)
            file_summaries.append(
                {
                    "logical_name": file_spec.logical_name,
                    "path": str(file_path),
                    "relative_path": f"bundles/{bundle_dir.name}/{file_name}",
                    "row_count": len(vendor_rows),
                    "fieldnames": list(fieldnames),
                }
            )

        marker = {
            "contract_name": vendor_profile.contract_name,
            "contract_version": vendor_profile.contract_version,
            "vendor_profile": vendor_profile.name,
            "files": {
                file_spec.logical_name: _bundle_file_name(file_spec.logical_name)
                for file_spec in contract.file_specs
            },
        }
        marker_path = bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER
        marker_path.write_text(yaml.safe_dump(marker, sort_keys=False), encoding="utf-8")

        bundle_summaries.append(
            {
                "bundle_id": f"{vendor_profile.source_system}_{vendor_profile.name}",
                "source_system": vendor_profile.source_system,
                "vendor_profile": vendor_profile.name,
                "bundle_dir": str(bundle_dir),
                "marker_path": str(marker_path),
                "files": file_summaries,
            }
        )
        manifest_source_bundles.append(
            {
                "bundle_id": f"{vendor_profile.source_system}_{vendor_profile.name}",
                "source_class": vendor_profile.source_system,
                "path": f"bundles/{bundle_dir.name}",
                "contract_name": vendor_profile.contract_name,
                "contract_version": vendor_profile.contract_version,
                "vendor_profile": vendor_profile.name,
            }
        )

    manifest = {
        "manifest_version": "1.0",
        "entity_type": "person",
        "batch_id": f"synthetic-public-safety-vendor-{profile}-{seed}",
        "landing_zone": {
            "kind": "local_filesystem",
            "base_path": ".",
        },
        "sources": [
            {
                "source_id": "source_a",
                "path": "landing/source_a.csv",
                "format": "csv",
                "schema_version": "person-v1",
                "required_columns": list(PERSON_HEADERS),
            },
            {
                "source_id": "source_b",
                "path": "landing/source_b.csv",
                "format": "csv",
                "schema_version": "person-v1",
                "required_columns": list(PERSON_HEADERS),
            },
        ],
        "source_bundles": manifest_source_bundles,
    }
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    summary = {
        "output_dir": str(resolved_output_dir),
        "seed_data_dir": str(seed_dir),
        "landing_dir": str(landing_dir),
        "manifest_path": str(manifest_path),
        "profile": profile,
        "seed": seed,
        "person_count_override": person_count_override,
        "cad_profiles": [profile.name for profile in selected_profiles if profile.source_system == "cad"],
        "rms_profiles": [profile.name for profile in selected_profiles if profile.source_system == "rms"],
        "source_bundle_count": len(bundle_summaries),
        "bundle_summaries": bundle_summaries,
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    return PublicSafetyVendorBatchGenerationResult(
        output_dir=resolved_output_dir,
        manifest_path=manifest_path,
        summary_path=summary_path,
        summary=summary,
    )
