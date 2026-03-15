from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
import yaml

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import (
    INCIDENT_HEADERS,
    INCIDENT_LINK_HEADERS,
    PERSON_HEADERS,
)
from etl_identity_engine.ingest.public_safety_contracts import (
    CAD_CALL_FOR_SERVICE_CONTRACT,
    RMS_REPORT_PERSON_CONTRACT,
    PUBLIC_SAFETY_CONTRACT_MARKER,
    PublicSafetyContractValidationError,
    validate_public_safety_contract_bundle,
)


def _write_csv(path: Path, rows: list[dict[str, str]], headers: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet(path: Path, rows: list[dict[str, str]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _person_row(*, source_record_id: str, person_entity_id: str, source_system: str) -> dict[str, str]:
    return {
        "source_record_id": source_record_id,
        "person_entity_id": person_entity_id,
        "source_system": source_system,
        "first_name": "Taylor",
        "last_name": "Jordan",
        "dob": "1985-03-12",
        "address": "123 Main Street",
        "city": "Columbus",
        "state": "OH",
        "postal_code": "43004",
        "phone": "5551234567",
        "updated_at": "2026-03-14T00:00:00Z",
        "is_conflict_variant": "false",
        "conflict_types": "",
    }


def _incident_row(*, incident_id: str, source_system: str) -> dict[str, str]:
    return {
        "incident_id": incident_id,
        "source_system": source_system,
        "occurred_at": "2026-03-14T12:00:00Z",
        "location": "100 WEST MAIN STREET",
        "city": "Columbus",
        "state": "OH",
    }


def _link_row(
    *,
    incident_person_link_id: str,
    incident_id: str,
    person_entity_id: str,
    source_record_id: str,
) -> dict[str, str]:
    return {
        "incident_person_link_id": incident_person_link_id,
        "incident_id": incident_id,
        "person_entity_id": person_entity_id,
        "source_record_id": source_record_id,
        "role": "REPORTING_PARTY",
    }


def _write_contract_manifest(
    bundle_dir: Path,
    *,
    contract_name: str,
    files: dict[str, str],
    mapping_overlay: str | None = None,
    vendor_profile: str | None = None,
) -> Path:
    payload: dict[str, object] = {
        "contract_name": contract_name,
        "contract_version": "v1",
        "files": files,
    }
    if mapping_overlay is not None:
        payload["mapping_overlay"] = mapping_overlay
    if vendor_profile is not None:
        payload["vendor_profile"] = vendor_profile
    marker_path = bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER
    marker_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return marker_path


def _write_mapping_overlay(
    bundle_dir: Path,
    *,
    relative_path: str,
    contract_name: str,
    files: dict[str, dict[str, dict[str, str]]],
) -> Path:
    overlay_path = bundle_dir / relative_path
    overlay_path.parent.mkdir(parents=True, exist_ok=True)
    overlay_path.write_text(
        yaml.safe_dump(
            {
                "overlay_version": "v1",
                "contract_name": contract_name,
                "contract_version": "v1",
                "files": files,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return overlay_path


def _build_bundle(
    tmp_path: Path,
    *,
    contract_name: str,
    person_extension: str = "csv",
    incident_extension: str = "csv",
    links_extension: str = "csv",
    person_rows: list[dict[str, str]] | None = None,
    incident_rows: list[dict[str, str]] | None = None,
    link_rows: list[dict[str, str]] | None = None,
) -> Path:
    bundle_dir = tmp_path / contract_name
    bundle_dir.mkdir(parents=True, exist_ok=True)

    if contract_name == CAD_CALL_FOR_SERVICE_CONTRACT.contract_name:
        spec = CAD_CALL_FOR_SERVICE_CONTRACT
    else:
        spec = RMS_REPORT_PERSON_CONTRACT

    source_system = spec.source_system
    person_rows = person_rows or [
        _person_row(
            source_record_id=f"{source_system.upper()}-1",
            person_entity_id="P-1",
            source_system=source_system,
        )
    ]
    incident_rows = incident_rows or [
        _incident_row(incident_id=f"{source_system.upper()}-INC-1", source_system=source_system)
    ]
    link_rows = link_rows or [
        _link_row(
            incident_person_link_id=f"{source_system.upper()}-LINK-1",
            incident_id=incident_rows[0]["incident_id"],
            person_entity_id=person_rows[0]["person_entity_id"],
            source_record_id=person_rows[0]["source_record_id"],
        )
    ]

    person_name = Path(spec.file_spec_by_name["person_records"].default_filename).with_suffix(
        f".{person_extension}"
    )
    incident_name = Path(spec.file_spec_by_name["incident_records"].default_filename).with_suffix(
        f".{incident_extension}"
    )
    links_name = Path(spec.file_spec_by_name["incident_person_links"].default_filename).with_suffix(
        f".{links_extension}"
    )

    if person_extension == "csv":
        _write_csv(bundle_dir / person_name, person_rows, PERSON_HEADERS)
    else:
        _write_parquet(bundle_dir / person_name, person_rows)

    if incident_extension == "csv":
        _write_csv(bundle_dir / incident_name, incident_rows, INCIDENT_HEADERS)
    else:
        _write_parquet(bundle_dir / incident_name, incident_rows)

    if links_extension == "csv":
        _write_csv(bundle_dir / links_name, link_rows, INCIDENT_LINK_HEADERS)
    else:
        _write_parquet(bundle_dir / links_name, link_rows)

    _write_contract_manifest(
        bundle_dir,
        contract_name=contract_name,
        files={
            "person_records": str(person_name).replace("\\", "/"),
            "incident_records": str(incident_name).replace("\\", "/"),
            "incident_person_links": str(links_name).replace("\\", "/"),
        },
    )
    return bundle_dir


def _build_vendor_overlay_bundle(tmp_path: Path, *, contract_name: str) -> tuple[Path, Path]:
    bundle_dir = tmp_path / f"{contract_name}_vendor"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    source_system = "cad" if contract_name == CAD_CALL_FOR_SERVICE_CONTRACT.contract_name else "rms"
    source_prefix = source_system.upper()
    person_rows = [
        {
            "vendor_person_record_key": f"{source_prefix}-1",
            "vendor_master_person_key": "P-1",
            "given_name": "Taylor",
            "surname": "Jordan",
            "date_of_birth": "1985-03-12",
            "street_line": "123 Main Street",
            "municipality": "Columbus",
            "region_code": "OH",
            "zip_code": "43004",
            "contact_phone": "5551234567",
            "record_last_updated_at": "2026-03-14T00:00:00Z",
            "variant_flag": "false",
            "variant_codes": "",
        }
    ]
    incident_rows = [
        {
            "vendor_incident_key": f"{source_prefix}-INC-1",
            "reported_at": "2026-03-14T12:00:00Z",
            "location_text": "100 WEST MAIN STREET",
            "municipality": "Columbus",
            "region_code": "OH",
        }
    ]
    link_rows = [
        {
            "vendor_link_key": f"{source_prefix}-LINK-1",
            "vendor_incident_key": incident_rows[0]["vendor_incident_key"],
            "vendor_master_person_key": person_rows[0]["vendor_master_person_key"],
            "vendor_person_record_key": person_rows[0]["vendor_person_record_key"],
            "involvement_role": "REPORTING_PARTY",
        }
    ]

    person_filename = "vendor_person_records.csv"
    incident_filename = "vendor_incident_records.csv"
    link_filename = "vendor_incident_person_links.csv"
    _write_csv(bundle_dir / person_filename, person_rows, tuple(person_rows[0]))
    _write_csv(bundle_dir / incident_filename, incident_rows, tuple(incident_rows[0]))
    _write_csv(bundle_dir / link_filename, link_rows, tuple(link_rows[0]))

    overlay_path = _write_mapping_overlay(
        bundle_dir,
        relative_path="overlays/vendor_columns.yml",
        contract_name=contract_name,
        files={
            "person_records": {
                "columns": {
                    "source_record_id": "vendor_person_record_key",
                    "person_entity_id": "vendor_master_person_key",
                    "first_name": "given_name",
                    "last_name": "surname",
                    "dob": "date_of_birth",
                    "address": "street_line",
                    "city": "municipality",
                    "state": "region_code",
                    "postal_code": "zip_code",
                    "phone": "contact_phone",
                    "updated_at": "record_last_updated_at",
                    "is_conflict_variant": "variant_flag",
                    "conflict_types": "variant_codes",
                },
                "defaults": {"source_system": source_system},
            },
            "incident_records": {
                "columns": {
                    "incident_id": "vendor_incident_key",
                    "occurred_at": "reported_at",
                    "location": "location_text",
                    "city": "municipality",
                    "state": "region_code",
                },
                "defaults": {"source_system": source_system},
            },
            "incident_person_links": {
                "columns": {
                    "incident_person_link_id": "vendor_link_key",
                    "incident_id": "vendor_incident_key",
                    "person_entity_id": "vendor_master_person_key",
                    "source_record_id": "vendor_person_record_key",
                    "role": "involvement_role",
                },
                "defaults": {},
            },
        },
    )
    marker_path = bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER
    marker_path.write_text(
        yaml.safe_dump(
            {
                "contract_name": contract_name,
                "contract_version": "v1",
                "mapping_overlay": "overlays/vendor_columns.yml",
                "files": {
                    "person_records": person_filename,
                    "incident_records": incident_filename,
                    "incident_person_links": link_filename,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return bundle_dir, overlay_path


def _packaged_vendor_profile_rows(profile_name: str) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    if profile_name == "cad_county_dispatch_v1":
        return (
            [
                {
                    "cad_person_key": "CAD-1",
                    "master_name_id": "P-1",
                    "given_name": "Taylor",
                    "family_name": "Jordan",
                    "birth_date": "1985-03-12",
                    "street_address": "123 Main Street",
                    "city_name": "Columbus",
                    "state_code": "OH",
                    "zip5": "43004",
                    "primary_phone": "5551234567",
                    "row_updated_at": "2026-03-14T00:00:00Z",
                    "variant_flag": "false",
                    "variant_codes": "",
                }
            ],
            [
                {
                    "cad_event_key": "CAD-INC-1",
                    "call_received_at": "2026-03-14T12:00:00Z",
                    "street_address": "100 WEST MAIN STREET",
                    "city_name": "Columbus",
                    "state_code": "OH",
                }
            ],
            [
                {
                    "cad_link_key": "CAD-LINK-1",
                    "cad_event_key": "CAD-INC-1",
                    "master_name_id": "P-1",
                    "cad_person_key": "CAD-1",
                    "party_role": "REPORTING_PARTY",
                }
            ],
        )
    if profile_name == "cad_records_management_v1":
        return (
            [
                {
                    "person_oid": "CAD-2",
                    "subject_uid": "P-2",
                    "first": "Jamie",
                    "last": "Lane",
                    "dob_yyyy_mm_dd": "1986-04-01",
                    "location_line1": "456 NORTH HIGH STREET",
                    "jurisdiction_city": "Columbus",
                    "jurisdiction_state": "OH",
                    "postal": "43004",
                    "phone_digits": "5559876543",
                    "last_edit_ts": "2026-03-14T01:00:00Z",
                    "conflict_flag": "false",
                    "conflict_reason_codes": "",
                }
            ],
            [
                {
                    "call_oid": "CAD-INC-2",
                    "dispatch_ts": "2026-03-14T13:00:00Z",
                    "place_text": "456 NORTH HIGH STREET",
                    "jurisdiction_city": "Columbus",
                    "jurisdiction_state": "OH",
                }
            ],
            [
                {
                    "call_person_oid": "CAD-LINK-2",
                    "call_oid": "CAD-INC-2",
                    "subject_uid": "P-2",
                    "person_oid": "CAD-2",
                    "participation_role": "SUBJECT",
                }
            ],
        )
    if profile_name == "rms_case_management_v1":
        return (
            [
                {
                    "rms_person_key": "RMS-1",
                    "master_subject_id": "P-10",
                    "subject_first_name": "Morgan",
                    "subject_last_name": "Lee",
                    "birth_date": "1984-02-18",
                    "residence_line1": "900 SOUTH FRONT STREET",
                    "residence_city": "Columbus",
                    "residence_state": "OH",
                    "residence_postal_code": "43004",
                    "contact_phone": "5551112222",
                    "row_last_modified_at": "2026-03-14T02:00:00Z",
                    "variant_flag": "false",
                    "variant_reason_codes": "",
                }
            ],
            [
                {
                    "report_key": "RMS-INC-1",
                    "report_received_at": "2026-03-14T14:00:00Z",
                    "offense_location": "900 SOUTH FRONT STREET",
                    "offense_city": "Columbus",
                    "offense_state": "OH",
                }
            ],
            [
                {
                    "report_person_link_key": "RMS-LINK-1",
                    "report_key": "RMS-INC-1",
                    "master_subject_id": "P-10",
                    "rms_person_key": "RMS-1",
                    "involvement_role": "VICTIM",
                }
            ],
        )
    if profile_name == "rms_records_bureau_v1":
        return (
            [
                {
                    "party_record_oid": "RMS-2",
                    "master_person_oid": "P-20",
                    "given_name": "Avery",
                    "family_name": "Brooks",
                    "dob_iso": "1979-11-05",
                    "address_text": "12 EAST STATE STREET",
                    "city_name": "Columbus",
                    "state_abbr": "OH",
                    "zip_code": "43004",
                    "phone_value": "5553334444",
                    "updated_timestamp": "2026-03-14T03:00:00Z",
                    "duplicate_flag": "false",
                    "duplicate_reason_codes": "",
                }
            ],
            [
                {
                    "report_number": "RMS-INC-2",
                    "incident_datetime": "2026-03-14T15:00:00Z",
                    "address_text": "12 EAST STATE STREET",
                    "city_name": "Columbus",
                    "state_abbr": "OH",
                }
            ],
            [
                {
                    "report_party_oid": "RMS-LINK-2",
                    "report_number": "RMS-INC-2",
                    "master_person_oid": "P-20",
                    "party_record_oid": "RMS-2",
                    "party_role": "ARRESTEE",
                }
            ],
        )
    raise AssertionError(f"Unsupported test vendor profile: {profile_name}")


def _build_packaged_vendor_profile_bundle(
    tmp_path: Path,
    *,
    profile_name: str,
    include_marker_vendor_profile: bool = True,
) -> Path:
    bundle_dir = tmp_path / profile_name
    bundle_dir.mkdir(parents=True, exist_ok=True)

    person_rows, incident_rows, link_rows = _packaged_vendor_profile_rows(profile_name)
    _write_csv(bundle_dir / "vendor_person_records.csv", person_rows, tuple(person_rows[0]))
    _write_csv(bundle_dir / "vendor_incident_records.csv", incident_rows, tuple(incident_rows[0]))
    _write_csv(bundle_dir / "vendor_incident_person_links.csv", link_rows, tuple(link_rows[0]))
    _write_contract_manifest(
        bundle_dir,
        contract_name=(
            CAD_CALL_FOR_SERVICE_CONTRACT.contract_name
            if profile_name.startswith("cad_")
            else RMS_REPORT_PERSON_CONTRACT.contract_name
        ),
        vendor_profile=profile_name if include_marker_vendor_profile else None,
        files={
            "person_records": "vendor_person_records.csv",
            "incident_records": "vendor_incident_records.csv",
            "incident_person_links": "vendor_incident_person_links.csv",
        },
    )
    return bundle_dir


def test_validate_cad_contract_bundle_accepts_valid_csv_bundle(tmp_path: Path) -> None:
    bundle_dir = _build_bundle(tmp_path, contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name)

    validated = validate_public_safety_contract_bundle(bundle_dir)

    assert validated.contract_name == CAD_CALL_FOR_SERVICE_CONTRACT.contract_name
    assert validated.contract_version == "v1"
    assert validated.source_system == "cad"
    assert {file.logical_name for file in validated.files} == {
        "person_records",
        "incident_records",
        "incident_person_links",
    }


def test_validate_rms_contract_bundle_accepts_valid_parquet_bundle(tmp_path: Path) -> None:
    bundle_dir = _build_bundle(
        tmp_path,
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
        person_extension="parquet",
        incident_extension="parquet",
        links_extension="parquet",
    )

    validated = validate_public_safety_contract_bundle(bundle_dir)

    assert validated.contract_name == RMS_REPORT_PERSON_CONTRACT.contract_name
    assert {file.format for file in validated.files} == {"parquet"}


def test_validate_public_safety_contract_accepts_vendor_overlay_bundle(tmp_path: Path) -> None:
    bundle_dir, overlay_path = _build_vendor_overlay_bundle(
        tmp_path,
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
    )

    validated = validate_public_safety_contract_bundle(bundle_dir)

    assert validated.contract_name == CAD_CALL_FOR_SERVICE_CONTRACT.contract_name
    assert validated.mapping_overlay_path == overlay_path.resolve()
    assert validated.files[0].source_fieldnames == (
        "vendor_person_record_key",
        "vendor_master_person_key",
        "given_name",
        "surname",
        "date_of_birth",
        "street_line",
        "municipality",
        "region_code",
        "zip_code",
        "contact_phone",
        "record_last_updated_at",
        "variant_flag",
        "variant_codes",
    )
    assert validated.files[0].fieldnames == PERSON_HEADERS


@pytest.mark.parametrize(
    "profile_name,expected_contract_name,expected_source_record_id,expected_incident_id",
    [
        ("cad_county_dispatch_v1", CAD_CALL_FOR_SERVICE_CONTRACT.contract_name, "CAD-1", "CAD-INC-1"),
        ("cad_records_management_v1", CAD_CALL_FOR_SERVICE_CONTRACT.contract_name, "CAD-2", "CAD-INC-2"),
        ("rms_case_management_v1", RMS_REPORT_PERSON_CONTRACT.contract_name, "RMS-1", "RMS-INC-1"),
        ("rms_records_bureau_v1", RMS_REPORT_PERSON_CONTRACT.contract_name, "RMS-2", "RMS-INC-2"),
    ],
)
def test_validate_public_safety_contract_accepts_packaged_vendor_profiles(
    tmp_path: Path,
    profile_name: str,
    expected_contract_name: str,
    expected_source_record_id: str,
    expected_incident_id: str,
) -> None:
    bundle_dir = _build_packaged_vendor_profile_bundle(tmp_path, profile_name=profile_name)

    validated = validate_public_safety_contract_bundle(bundle_dir)

    assert validated.contract_name == expected_contract_name
    assert validated.vendor_profile == profile_name
    assert validated.mapping_overlay_relative_path is None
    assert validated.files[0].fieldnames == PERSON_HEADERS
    assert validated.files[1].fieldnames == INCIDENT_HEADERS
    assert validated.files[2].fieldnames == INCIDENT_LINK_HEADERS
    assert validated.files[0].rows[0]["source_record_id"] == expected_source_record_id
    assert validated.files[1].rows[0]["incident_id"] == expected_incident_id


@pytest.mark.parametrize(
    "profile_name,missing_column",
    [
        ("cad_county_dispatch_v1", "given_name"),
        ("cad_records_management_v1", "first"),
        ("rms_case_management_v1", "subject_first_name"),
        ("rms_records_bureau_v1", "given_name"),
    ],
)
def test_validate_public_safety_contract_rejects_invalid_packaged_vendor_profiles(
    tmp_path: Path,
    profile_name: str,
    missing_column: str,
) -> None:
    bundle_dir = _build_packaged_vendor_profile_bundle(tmp_path, profile_name=profile_name)
    person_path = bundle_dir / "vendor_person_records.csv"
    person_rows = []
    with person_path.open("r", encoding="utf-8", newline="") as handle:
        person_rows = list(csv.DictReader(handle))
    for row in person_rows:
        row.pop(missing_column, None)
    _write_csv(person_path, person_rows, tuple(column for column in person_rows[0] if column != missing_column))

    with pytest.raises(
        PublicSafetyContractValidationError,
        match=rf"files.person_records references missing source columns: {missing_column}",
    ):
        validate_public_safety_contract_bundle(bundle_dir)


def test_validate_public_safety_contract_rejects_marker_with_overlay_and_vendor_profile(
    tmp_path: Path,
) -> None:
    bundle_dir = _build_packaged_vendor_profile_bundle(
        tmp_path,
        profile_name="cad_county_dispatch_v1",
        include_marker_vendor_profile=False,
    )
    _write_contract_manifest(
        bundle_dir,
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
        mapping_overlay="overlays/vendor_columns.yml",
        vendor_profile="cad_county_dispatch_v1",
        files={
            "person_records": "vendor_person_records.csv",
            "incident_records": "vendor_incident_records.csv",
            "incident_person_links": "vendor_incident_person_links.csv",
        },
    )

    with pytest.raises(
        PublicSafetyContractValidationError,
        match="contract manifest cannot define both mapping_overlay and vendor_profile",
    ):
        validate_public_safety_contract_bundle(bundle_dir)


def test_validate_public_safety_contract_rejects_missing_marker(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "cad_call_for_service"
    bundle_dir.mkdir()

    with pytest.raises(
        PublicSafetyContractValidationError,
        match="missing contract_manifest.yml",
    ):
        validate_public_safety_contract_bundle(bundle_dir)


def test_validate_public_safety_contract_rejects_missing_required_file(tmp_path: Path) -> None:
    bundle_dir = _build_bundle(tmp_path, contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name)
    (bundle_dir / "cad_person_records.csv").unlink()

    with pytest.raises(
        PublicSafetyContractValidationError,
        match=r"contract file 'person_records' not found: cad_person_records.csv",
    ):
        validate_public_safety_contract_bundle(bundle_dir)


def test_validate_public_safety_contract_rejects_missing_required_columns(tmp_path: Path) -> None:
    bundle_dir = tmp_path / CAD_CALL_FOR_SERVICE_CONTRACT.contract_name
    bundle_dir.mkdir()
    _write_csv(
        bundle_dir / "cad_person_records.csv",
        [_person_row(source_record_id="CAD-1", person_entity_id="P-1", source_system="cad")],
        PERSON_HEADERS,
    )

    broken_incident = _incident_row(incident_id="CAD-INC-1", source_system="cad")
    broken_incident.pop("location")
    _write_csv(
        bundle_dir / "cad_incident_records.csv",
        [broken_incident],
        tuple(column for column in INCIDENT_HEADERS if column != "location"),
    )
    _write_csv(
        bundle_dir / "cad_incident_person_links.csv",
        [
            _link_row(
                incident_person_link_id="CAD-LINK-1",
                incident_id="CAD-INC-1",
                person_entity_id="P-1",
                source_record_id="CAD-1",
            )
        ],
        INCIDENT_LINK_HEADERS,
    )
    _write_contract_manifest(
        bundle_dir,
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
        files={
            "person_records": "cad_person_records.csv",
            "incident_records": "cad_incident_records.csv",
            "incident_person_links": "cad_incident_person_links.csv",
        },
    )

    with pytest.raises(
        PublicSafetyContractValidationError,
        match=r"contract file 'incident_records' is missing required columns: location",
    ):
        validate_public_safety_contract_bundle(bundle_dir)


def test_validate_public_safety_contract_rejects_mismatched_source_system(tmp_path: Path) -> None:
    bundle_dir = _build_bundle(
        tmp_path,
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
        person_rows=[_person_row(source_record_id="RMS-1", person_entity_id="P-1", source_system="cad")],
    )

    with pytest.raises(
        PublicSafetyContractValidationError,
        match="person_records contains source_system values that do not match the contract: cad",
    ):
        validate_public_safety_contract_bundle(bundle_dir)


def test_validate_public_safety_contract_rejects_broken_link_references(tmp_path: Path) -> None:
    bundle_dir = _build_bundle(
        tmp_path,
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
        link_rows=[
            _link_row(
                incident_person_link_id="CAD-LINK-1",
                incident_id="CAD-INC-1",
                person_entity_id="P-999",
                source_record_id="CAD-1",
            )
        ],
    )

    with pytest.raises(
        PublicSafetyContractValidationError,
        match="incident_person_links contains person_entity_id values that do not match the referenced source record",
    ):
        validate_public_safety_contract_bundle(bundle_dir)


def test_validate_public_safety_contract_cli_writes_summary_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bundle_dir = _build_bundle(tmp_path, contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name)

    assert (
        main(
            [
                "validate-public-safety-contract",
                "--bundle-dir",
                str(bundle_dir),
            ]
        )
        == 0
    )

    summary = json.loads(capsys.readouterr().out)
    assert summary["contract_name"] == CAD_CALL_FOR_SERVICE_CONTRACT.contract_name
    assert summary["source_system"] == "cad"
    assert set(summary["files"]) == {
        "person_records",
        "incident_records",
        "incident_person_links",
    }


def test_validate_public_safety_contract_cli_accepts_packaged_vendor_profile(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bundle_dir = _build_packaged_vendor_profile_bundle(
        tmp_path,
        profile_name="cad_county_dispatch_v1",
        include_marker_vendor_profile=False,
    )

    assert (
        main(
            [
                "validate-public-safety-contract",
                "--bundle-dir",
                str(bundle_dir),
                "--vendor-profile",
                "cad_county_dispatch_v1",
            ]
        )
        == 0
    )

    summary = json.loads(capsys.readouterr().out)
    assert summary["vendor_profile"] == "cad_county_dispatch_v1"
    assert summary["files"]["person_records"]["fieldnames"] == list(PERSON_HEADERS)


def test_validate_public_safety_contract_cli_accepts_packaged_rms_vendor_profile(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bundle_dir = _build_packaged_vendor_profile_bundle(
        tmp_path,
        profile_name="rms_case_management_v1",
        include_marker_vendor_profile=False,
    )

    assert (
        main(
            [
                "validate-public-safety-contract",
                "--bundle-dir",
                str(bundle_dir),
                "--vendor-profile",
                "rms_case_management_v1",
            ]
        )
        == 0
    )

    summary = json.loads(capsys.readouterr().out)
    assert summary["contract_name"] == RMS_REPORT_PERSON_CONTRACT.contract_name
    assert summary["vendor_profile"] == "rms_case_management_v1"
    assert summary["files"]["incident_records"]["fieldnames"] == list(INCIDENT_HEADERS)
