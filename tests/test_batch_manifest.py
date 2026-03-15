from __future__ import annotations

import csv
from io import BytesIO
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
    PUBLIC_SAFETY_CONTRACT_MARKER,
    RMS_REPORT_PERSON_CONTRACT,
)
from etl_identity_engine.ingest.manifest import (
    BatchManifestValidationError,
    resolve_batch_manifest,
)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet(path: Path, rows: list[dict[str, str]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _person_row(*, source_record_id: str, person_entity_id: str, source_system: str) -> dict[str, str]:
    return {
        "source_record_id": source_record_id,
        "person_entity_id": person_entity_id,
        "source_system": source_system,
        "first_name": "John",
        "last_name": "Smith",
        "dob": "1985-03-12",
        "address": "123 Main St.",
        "city": "Columbus",
        "state": "OH",
        "postal_code": "43004",
        "phone": "(555) 123-4567",
        "updated_at": "2025-01-01T00:00:00Z",
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


def _write_manifest(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body.strip() + "\n", encoding="utf-8")
    return path


def _write_memory_csv(uri: str, rows: list[dict[str, str]]) -> None:
    import fsspec

    with fsspec.open(uri, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_memory_parquet(uri: str, rows: list[dict[str, str]]) -> None:
    import fsspec
    import pyarrow as pa
    import pyarrow.parquet as pq

    buffer = BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    with fsspec.open(uri, "wb") as handle:
        handle.write(buffer.getvalue())


def _manifest_body(
    *,
    source_a_path: str,
    source_b_path: str,
    landing_zone_kind: str = "local_filesystem",
    base_location_key: str = "base_path",
    base_location_value: str = "./landing",
    storage_options: str = "",
    source_bundles_block: str = "",
) -> str:
    required_columns = "\n".join(f"        - {column}" for column in PERSON_HEADERS)
    return f"""
manifest_version: "1.0"
entity_type: person
batch_id: inbound-2026-03-13
landing_zone:
  kind: {landing_zone_kind}
  {base_location_key}: {base_location_value}
{storage_options}
sources:
  - source_id: source_a
    path: {source_a_path}
    format: csv
    schema_version: person-v1
    required_columns:
{required_columns}
  - source_id: source_b
    path: {source_b_path}
    format: parquet
    schema_version: person-v1
    required_columns:
{required_columns}
{source_bundles_block}
"""


def _write_contract_manifest(bundle_dir: Path, *, contract_name: str, files: dict[str, str]) -> None:
    (bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER).write_text(
        yaml.safe_dump(
            {
                "contract_name": contract_name,
                "contract_version": "v1",
                "files": files,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _write_mapping_overlay(
    root_dir: Path,
    *,
    relative_path: str,
    contract_name: str,
    files: dict[str, dict[str, dict[str, str]]],
) -> Path:
    overlay_path = root_dir / relative_path
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


def _build_public_safety_bundle(
    root_dir: Path,
    *,
    contract_name: str,
    extension: str = "csv",
) -> Path:
    root_dir.mkdir(parents=True, exist_ok=True)
    if contract_name == CAD_CALL_FOR_SERVICE_CONTRACT.contract_name:
        contract = CAD_CALL_FOR_SERVICE_CONTRACT
    else:
        contract = RMS_REPORT_PERSON_CONTRACT

    source_system = contract.source_system
    person_rows = [
        _person_row(
            source_record_id=f"{source_system.upper()}-1",
            person_entity_id="PS-1",
            source_system=source_system,
        )
    ]
    incident_rows = [
        _incident_row(
            incident_id=f"{source_system.upper()}-INC-1",
            source_system=source_system,
        )
    ]
    link_rows = [
        _link_row(
            incident_person_link_id=f"{source_system.upper()}-LINK-1",
            incident_id=incident_rows[0]["incident_id"],
            person_entity_id=person_rows[0]["person_entity_id"],
            source_record_id=person_rows[0]["source_record_id"],
        )
    ]

    person_filename = Path(contract.file_spec_by_name["person_records"].default_filename).with_suffix(
        f".{extension}"
    )
    incident_filename = Path(
        contract.file_spec_by_name["incident_records"].default_filename
    ).with_suffix(f".{extension}")
    link_filename = Path(
        contract.file_spec_by_name["incident_person_links"].default_filename
    ).with_suffix(f".{extension}")

    if extension == "csv":
        _write_csv(root_dir / person_filename, person_rows)
        _write_csv(root_dir / incident_filename, incident_rows)
        _write_csv(root_dir / link_filename, link_rows)
    else:
        _write_parquet(root_dir / person_filename, person_rows)
        _write_parquet(root_dir / incident_filename, incident_rows)
        _write_parquet(root_dir / link_filename, link_rows)

    _write_contract_manifest(
        root_dir,
        contract_name=contract_name,
        files={
            "person_records": str(person_filename).replace("\\", "/"),
            "incident_records": str(incident_filename).replace("\\", "/"),
            "incident_person_links": str(link_filename).replace("\\", "/"),
        },
    )
    return root_dir


def _build_vendor_public_safety_bundle(
    root_dir: Path,
    *,
    contract_name: str,
    include_marker_overlay: bool,
) -> tuple[Path, Path]:
    root_dir.mkdir(parents=True, exist_ok=True)
    if contract_name == CAD_CALL_FOR_SERVICE_CONTRACT.contract_name:
        contract = CAD_CALL_FOR_SERVICE_CONTRACT
    else:
        contract = RMS_REPORT_PERSON_CONTRACT

    source_system = contract.source_system
    source_prefix = source_system.upper()
    person_rows = [
        {
            "vendor_person_record_key": f"{source_prefix}-1",
            "vendor_master_person_key": "PS-1",
            "given_name": "Jamie",
            "surname": "Lane",
            "date_of_birth": "1985-03-12",
            "street_line": "123 Main St",
            "municipality": "Columbus",
            "region_code": "OH",
            "zip_code": "43004",
            "contact_phone": "(555) 123-4567",
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
    _write_csv(root_dir / person_filename, person_rows)
    _write_csv(root_dir / incident_filename, incident_rows)
    _write_csv(root_dir / link_filename, link_rows)

    overlay_path = _write_mapping_overlay(
        root_dir,
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
    manifest_payload: dict[str, object] = {
        "contract_name": contract_name,
        "contract_version": "v1",
        "files": {
            "person_records": person_filename,
            "incident_records": incident_filename,
            "incident_person_links": link_filename,
        },
    }
    if include_marker_overlay:
        manifest_payload["mapping_overlay"] = "overlays/vendor_columns.yml"
    (root_dir / PUBLIC_SAFETY_CONTRACT_MARKER).write_text(
        yaml.safe_dump(manifest_payload, sort_keys=False),
        encoding="utf-8",
    )
    return root_dir, overlay_path


def _build_packaged_profile_public_safety_bundle(
    root_dir: Path,
    *,
    profile_name: str,
    include_marker_vendor_profile: bool,
) -> Path:
    root_dir.mkdir(parents=True, exist_ok=True)
    if profile_name == "cad_county_dispatch_v1":
        person_rows = [
            {
                "cad_person_key": "CAD-1",
                "master_name_id": "PS-1",
                "given_name": "Jamie",
                "family_name": "Lane",
                "birth_date": "1985-03-12",
                "street_address": "123 Main St",
                "city_name": "Columbus",
                "state_code": "OH",
                "zip5": "43004",
                "primary_phone": "(555) 123-4567",
                "row_updated_at": "2026-03-14T00:00:00Z",
                "variant_flag": "false",
                "variant_codes": "",
            }
        ]
        incident_rows = [
            {
                "cad_event_key": "CAD-INC-1",
                "call_received_at": "2026-03-14T12:00:00Z",
                "street_address": "100 WEST MAIN STREET",
                "city_name": "Columbus",
                "state_code": "OH",
            }
        ]
        link_rows = [
            {
                "cad_link_key": "CAD-LINK-1",
                "cad_event_key": "CAD-INC-1",
                "master_name_id": "PS-1",
                "cad_person_key": "CAD-1",
                "party_role": "REPORTING_PARTY",
            }
        ]
    elif profile_name == "cad_records_management_v1":
        person_rows = [
            {
                "person_oid": "CAD-2",
                "subject_uid": "PS-2",
                "first": "Alex",
                "last": "Rivera",
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
        ]
        incident_rows = [
            {
                "call_oid": "CAD-INC-2",
                "dispatch_ts": "2026-03-14T13:00:00Z",
                "place_text": "456 NORTH HIGH STREET",
                "jurisdiction_city": "Columbus",
                "jurisdiction_state": "OH",
            }
        ]
        link_rows = [
            {
                "call_person_oid": "CAD-LINK-2",
                "call_oid": "CAD-INC-2",
                "subject_uid": "PS-2",
                "person_oid": "CAD-2",
                "participation_role": "SUBJECT",
            }
        ]
    elif profile_name == "rms_case_management_v1":
        person_rows = [
            {
                "rms_person_key": "RMS-1",
                "master_subject_id": "PS-10",
                "subject_first_name": "Jordan",
                "subject_last_name": "Mills",
                "birth_date": "1985-03-12",
                "residence_line1": "700 WEST TOWN STREET",
                "residence_city": "Columbus",
                "residence_state": "OH",
                "residence_postal_code": "43004",
                "contact_phone": "(555) 123-4567",
                "row_last_modified_at": "2026-03-14T00:00:00Z",
                "variant_flag": "false",
                "variant_reason_codes": "",
            }
        ]
        incident_rows = [
            {
                "report_key": "RMS-INC-1",
                "report_received_at": "2026-03-14T12:00:00Z",
                "offense_location": "700 WEST TOWN STREET",
                "offense_city": "Columbus",
                "offense_state": "OH",
            }
        ]
        link_rows = [
            {
                "report_person_link_key": "RMS-LINK-1",
                "report_key": "RMS-INC-1",
                "master_subject_id": "PS-10",
                "rms_person_key": "RMS-1",
                "involvement_role": "WITNESS",
            }
        ]
    elif profile_name == "rms_records_bureau_v1":
        person_rows = [
            {
                "party_record_oid": "RMS-2",
                "master_person_oid": "PS-20",
                "given_name": "Casey",
                "family_name": "Harper",
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
        ]
        incident_rows = [
            {
                "report_number": "RMS-INC-2",
                "incident_datetime": "2026-03-14T15:00:00Z",
                "address_text": "12 EAST STATE STREET",
                "city_name": "Columbus",
                "state_abbr": "OH",
            }
        ]
        link_rows = [
            {
                "report_party_oid": "RMS-LINK-2",
                "report_number": "RMS-INC-2",
                "master_person_oid": "PS-20",
                "party_record_oid": "RMS-2",
                "party_role": "SUSPECT",
            }
        ]
    else:
        raise AssertionError(f"Unsupported test vendor profile: {profile_name}")

    _write_csv(root_dir / "vendor_person_records.csv", person_rows)
    _write_csv(root_dir / "vendor_incident_records.csv", incident_rows)
    _write_csv(root_dir / "vendor_incident_person_links.csv", link_rows)

    manifest_payload: dict[str, object] = {
        "contract_name": (
            CAD_CALL_FOR_SERVICE_CONTRACT.contract_name
            if profile_name.startswith("cad_")
            else RMS_REPORT_PERSON_CONTRACT.contract_name
        ),
        "contract_version": "v1",
        "files": {
            "person_records": "vendor_person_records.csv",
            "incident_records": "vendor_incident_records.csv",
            "incident_person_links": "vendor_incident_person_links.csv",
        },
    }
    if include_marker_vendor_profile:
        manifest_payload["vendor_profile"] = profile_name
    (root_dir / PUBLIC_SAFETY_CONTRACT_MARKER).write_text(
        yaml.safe_dump(manifest_payload, sort_keys=False),
        encoding="utf-8",
    )
    return root_dir


def _source_bundles_block(*, cad_path: str, rms_path: str) -> str:
    return f"""
source_bundles:
  - bundle_id: cad_primary
    source_class: cad
    path: {cad_path}
    contract_name: cad_call_for_service
    contract_version: v1
  - bundle_id: rms_primary
    source_class: rms
    path: {rms_path}
    contract_name: rms_report_person
    contract_version: v1
"""


def _source_bundles_block_with_overlay(
    *,
    cad_path: str,
    rms_path: str,
    rms_overlay_path: str,
) -> str:
    return f"""
source_bundles:
  - bundle_id: cad_primary
    source_class: cad
    path: {cad_path}
    contract_name: cad_call_for_service
    contract_version: v1
  - bundle_id: rms_primary
    source_class: rms
    path: {rms_path}
    contract_name: rms_report_person
    contract_version: v1
    mapping_overlay: {rms_overlay_path}
"""


def _source_bundles_block_with_vendor_profile(
    *,
    cad_path: str,
    rms_path: str,
    cad_vendor_profile: str,
) -> str:
    return f"""
source_bundles:
  - bundle_id: cad_primary
    source_class: cad
    path: {cad_path}
    contract_name: cad_call_for_service
    contract_version: v1
    vendor_profile: {cad_vendor_profile}
  - bundle_id: rms_primary
    source_class: rms
    path: {rms_path}
    contract_name: rms_report_person
    contract_version: v1
"""


def test_resolve_batch_manifest_validates_and_resolves_local_sources(tmp_path: Path) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        _manifest_body(source_a_path="agency_a.csv", source_b_path="agency_b.parquet"),
    )

    resolved = resolve_batch_manifest(manifest_path)

    assert resolved.manifest.batch_id == "inbound-2026-03-13"
    assert resolved.input_paths == (
        str(landing_dir / "agency_a.csv"),
        str(landing_dir / "agency_b.parquet"),
    )
    assert len(resolved.all_rows()) == 2


def test_normalize_cli_accepts_manifest_inputs(tmp_path: Path) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        _manifest_body(source_a_path="agency_a.csv", source_b_path="agency_b.parquet"),
    )
    output_path = tmp_path / "normalized" / "normalized_person_records.csv"

    assert (
        main(
            [
                "normalize",
                "--manifest",
                str(manifest_path),
                "--output",
                str(output_path),
            ]
        )
        == 0
    )

    rows = _read_csv(output_path)

    assert len(rows) == 2
    assert {row["source_record_id"] for row in rows} == {"A-1", "B-1"}
    assert rows[0]["canonical_name"] == "JOHN SMITH"


def test_normalize_manifest_rejects_missing_required_columns_without_partial_output(
    tmp_path: Path,
) -> None:
    landing_dir = tmp_path / "landing"
    broken_row = _person_row(
        source_record_id="A-1",
        person_entity_id="P-1",
        source_system="source_a",
    )
    broken_row.pop("phone")
    _write_csv(landing_dir / "agency_a.csv", [broken_row])
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        _manifest_body(source_a_path="agency_a.csv", source_b_path="agency_b.parquet"),
    )
    output_path = tmp_path / "normalized" / "normalized_person_records.csv"

    with pytest.raises(
        BatchManifestValidationError,
        match=r"source 'source_a' is missing required columns: phone",
    ):
        main(
            [
                "normalize",
                "--manifest",
                str(manifest_path),
                "--output",
                str(output_path),
            ]
        )

    assert not output_path.exists()


def test_normalize_manifest_rejects_mismatched_source_identifier(tmp_path: Path) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="wrong_source")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        _manifest_body(source_a_path="agency_a.csv", source_b_path="agency_b.parquet"),
    )

    with pytest.raises(
        BatchManifestValidationError,
        match=r"source 'source_a' contains source_system values that do not match the manifest: wrong_source",
    ):
        main(
            [
                "normalize",
                "--manifest",
                str(manifest_path),
                "--output",
                str(tmp_path / "normalized.csv"),
            ]
        )


def test_run_all_supports_manifest_inputs_without_synthetic_generation(tmp_path: Path) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        _manifest_body(source_a_path="agency_a.csv", source_b_path="agency_b.parquet"),
    )
    base_dir = tmp_path / "run"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--manifest",
                str(manifest_path),
            ]
        )
        == 0
    )

    assert (base_dir / "data" / "normalized" / "normalized_person_records.csv").exists()
    assert (base_dir / "data" / "golden" / "golden_person_records.csv").exists()
    assert not (base_dir / "data" / "synthetic_sources").exists()


def test_resolve_batch_manifest_validates_named_public_safety_source_bundles(
    tmp_path: Path,
) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir = _build_public_safety_bundle(
        landing_dir / "cad_bundle",
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
    )
    rms_bundle_dir = _build_public_safety_bundle(
        landing_dir / "rms_bundle",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
        extension="parquet",
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest-with-bundles.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=_source_bundles_block(
                cad_path=cad_bundle_dir.name,
                rms_path=rms_bundle_dir.name,
            ),
        ),
    )

    resolved = resolve_batch_manifest(manifest_path)

    assert len(resolved.sources) == 2
    assert len(resolved.source_bundles) == 2
    assert [bundle.spec.bundle_id for bundle in resolved.source_bundles] == [
        "cad_primary",
        "rms_primary",
    ]
    assert [bundle.contract_name for bundle in resolved.source_bundles] == [
        CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
        RMS_REPORT_PERSON_CONTRACT.contract_name,
    ]
    assert [file.row_count for file in resolved.source_bundles[0].files] == [1, 1, 1]
    assert resolved.source_bundles[0].files[1].rows[0]["source_system"] == "cad"
    assert resolved.source_bundles[1].files[1].rows[0]["source_system"] == "rms"
    assert resolved.source_bundles[0].files[0].diff_report["overlay_mode"] == "canonical_passthrough"


def test_resolve_batch_manifest_supports_vendor_mapped_public_safety_bundle_overlays(
    tmp_path: Path,
) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir = _build_public_safety_bundle(
        landing_dir / "cad_bundle",
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
    )
    rms_bundle_dir, rms_overlay_path = _build_vendor_public_safety_bundle(
        landing_dir / "rms_vendor_bundle",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
        include_marker_overlay=False,
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest-with-vendor-bundles.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=_source_bundles_block_with_overlay(
                cad_path=cad_bundle_dir.name,
                rms_path=rms_bundle_dir.name,
                rms_overlay_path=str(rms_overlay_path.relative_to(rms_bundle_dir)).replace("\\", "/"),
            ),
        ),
    )

    resolved = resolve_batch_manifest(manifest_path)

    assert len(resolved.source_bundles) == 2
    rms_bundle = next(bundle for bundle in resolved.source_bundles if bundle.spec.bundle_id == "rms_primary")
    assert rms_bundle.mapping_overlay_reference == str(rms_overlay_path)
    assert [file.fieldnames for file in rms_bundle.files] == [
        PERSON_HEADERS,
        INCIDENT_HEADERS,
        INCIDENT_LINK_HEADERS,
    ]
    assert rms_bundle.files[0].rows[0]["source_record_id"] == "RMS-1"
    assert rms_bundle.files[1].rows[0]["incident_id"] == "RMS-INC-1"
    assert rms_bundle.files[0].diff_report["overlay_mode"] == "mapping_overlay"
    assert rms_bundle.files[0].diff_report["missing_required_canonical_fields"] == []


def test_resolve_batch_manifest_supports_packaged_cad_vendor_profiles(
    tmp_path: Path,
) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir = _build_packaged_profile_public_safety_bundle(
        landing_dir / "cad_vendor_profile_bundle",
        profile_name="cad_county_dispatch_v1",
        include_marker_vendor_profile=False,
    )
    rms_bundle_dir = _build_public_safety_bundle(
        landing_dir / "rms_bundle",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest-with-vendor-profile.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=_source_bundles_block_with_vendor_profile(
                cad_path=cad_bundle_dir.name,
                rms_path=rms_bundle_dir.name,
                cad_vendor_profile="cad_county_dispatch_v1",
            ),
        ),
    )

    resolved = resolve_batch_manifest(manifest_path)

    cad_bundle = next(bundle for bundle in resolved.source_bundles if bundle.spec.bundle_id == "cad_primary")
    assert cad_bundle.vendor_profile == "cad_county_dispatch_v1"
    assert cad_bundle.mapping_overlay_reference is None
    assert cad_bundle.files[0].rows[0]["source_record_id"] == "CAD-1"
    assert cad_bundle.files[1].rows[0]["incident_id"] == "CAD-INC-1"
    assert cad_bundle.files[0].diff_report["overlay_mode"] == "vendor_profile"


def test_resolve_batch_manifest_rejects_source_bundle_with_overlay_and_vendor_profile(
    tmp_path: Path,
) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir = _build_packaged_profile_public_safety_bundle(
        landing_dir / "cad_vendor_profile_bundle",
        profile_name="cad_county_dispatch_v1",
        include_marker_vendor_profile=False,
    )
    rms_bundle_dir = _build_public_safety_bundle(
        landing_dir / "rms_bundle",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
    )
    manifest_path = _write_manifest(
        tmp_path / "manifest-with-invalid-vendor-profile.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=f"""
source_bundles:
  - bundle_id: cad_primary
    source_class: cad
    path: {cad_bundle_dir.name}
    contract_name: cad_call_for_service
    contract_version: v1
    mapping_overlay: overlays/vendor_columns.yml
    vendor_profile: cad_county_dispatch_v1
  - bundle_id: rms_primary
    source_class: rms
    path: {rms_bundle_dir.name}
    contract_name: rms_report_person
    contract_version: v1
""",
        ),
    )

    with pytest.raises(
        BatchManifestValidationError,
        match=r"source_bundles\[0\] cannot define both mapping_overlay and vendor_profile",
    ):
        resolve_batch_manifest(manifest_path)


def test_normalize_manifest_rejects_invalid_public_safety_source_bundle_without_partial_output(
    tmp_path: Path,
) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir = _build_public_safety_bundle(
        landing_dir / "cad_bundle",
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
    )
    rms_bundle_dir = _build_public_safety_bundle(
        landing_dir / "rms_bundle",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
    )
    (cad_bundle_dir / "cad_incident_person_links.csv").unlink()
    manifest_path = _write_manifest(
        tmp_path / "manifest-with-invalid-bundles.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=_source_bundles_block(
                cad_path=cad_bundle_dir.name,
                rms_path=rms_bundle_dir.name,
            ),
        ),
    )
    output_path = tmp_path / "normalized" / "normalized_person_records.csv"

    with pytest.raises(
        BatchManifestValidationError,
        match=r"source_bundle 'cad_primary' failed contract validation:",
    ):
        main(
            [
                "normalize",
                "--manifest",
                str(manifest_path),
                "--output",
                str(output_path),
            ]
        )

    assert not output_path.exists()


def test_run_all_supports_manifest_inputs_with_mixed_cad_rms_source_bundles(tmp_path: Path) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir = _build_public_safety_bundle(
        landing_dir / "cad_bundle",
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
    )
    rms_bundle_dir = _build_public_safety_bundle(
        landing_dir / "rms_bundle",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
    )
    manifest_path = _write_manifest(
        tmp_path / "mixed-bundles-manifest.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=_source_bundles_block(
                cad_path=cad_bundle_dir.name,
                rms_path=rms_bundle_dir.name,
            ),
        ),
    )
    base_dir = tmp_path / "mixed-run"

    assert main(["run-all", "--base-dir", str(base_dir), "--manifest", str(manifest_path)]) == 0

    assert (base_dir / "data" / "normalized" / "normalized_person_records.csv").exists()
    assert (base_dir / "data" / "golden" / "golden_person_records.csv").exists()


def test_run_all_supports_vendor_mapped_public_safety_source_bundles(tmp_path: Path) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir, _cad_overlay = _build_vendor_public_safety_bundle(
        landing_dir / "cad_vendor_bundle",
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
        include_marker_overlay=True,
    )
    rms_bundle_dir = _build_public_safety_bundle(
        landing_dir / "rms_bundle",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
    )
    manifest_path = _write_manifest(
        tmp_path / "vendor-run-all-manifest.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=_source_bundles_block(
                cad_path=cad_bundle_dir.name,
                rms_path=rms_bundle_dir.name,
            ),
        ),
    )
    base_dir = tmp_path / "vendor-run"

    assert main(["run-all", "--base-dir", str(base_dir), "--manifest", str(manifest_path)]) == 0

    normalized_rows = _read_csv(base_dir / "data" / "normalized" / "normalized_person_records.csv")
    assert {row["source_record_id"] for row in normalized_rows} == {"A-1", "B-1"}
    assert (base_dir / "data" / "golden" / "golden_person_records.csv").exists()


def test_run_all_supports_packaged_cad_vendor_profile_source_bundles(tmp_path: Path) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir = _build_packaged_profile_public_safety_bundle(
        landing_dir / "cad_vendor_profile_bundle",
        profile_name="cad_records_management_v1",
        include_marker_vendor_profile=False,
    )
    rms_bundle_dir = _build_public_safety_bundle(
        landing_dir / "rms_bundle",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
    )
    manifest_path = _write_manifest(
        tmp_path / "vendor-profile-run-all-manifest.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=_source_bundles_block_with_vendor_profile(
                cad_path=cad_bundle_dir.name,
                rms_path=rms_bundle_dir.name,
                cad_vendor_profile="cad_records_management_v1",
            ),
        ),
    )
    base_dir = tmp_path / "vendor-profile-run"

    assert main(["run-all", "--base-dir", str(base_dir), "--manifest", str(manifest_path)]) == 0

    assert (base_dir / "data" / "golden" / "golden_person_records.csv").exists()
    public_safety_rows = _read_csv(base_dir / "data" / "public_safety_demo" / "incident_identity_view.csv")
    assert {row["incident_id"] for row in public_safety_rows} == {"CAD-INC-2", "RMS-INC-1"}


def test_run_all_supports_packaged_rms_vendor_profile_source_bundles(tmp_path: Path) -> None:
    landing_dir = tmp_path / "landing"
    _write_csv(
        landing_dir / "agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_parquet(
        landing_dir / "agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    cad_bundle_dir = _build_public_safety_bundle(
        landing_dir / "cad_bundle",
        contract_name=CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
    )
    rms_bundle_dir = _build_packaged_profile_public_safety_bundle(
        landing_dir / "rms_vendor_profile_bundle",
        profile_name="rms_records_bureau_v1",
        include_marker_vendor_profile=False,
    )
    manifest_path = _write_manifest(
        tmp_path / "rms-vendor-profile-run-all-manifest.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            source_bundles_block=f"""
source_bundles:
  - bundle_id: cad_primary
    source_class: cad
    path: {cad_bundle_dir.name}
    contract_name: cad_call_for_service
    contract_version: v1
  - bundle_id: rms_primary
    source_class: rms
    path: {rms_bundle_dir.name}
    contract_name: rms_report_person
    contract_version: v1
    vendor_profile: rms_records_bureau_v1
""",
        ),
    )
    base_dir = tmp_path / "rms-vendor-profile-run"

    assert main(["run-all", "--base-dir", str(base_dir), "--manifest", str(manifest_path)]) == 0

    public_safety_rows = _read_csv(base_dir / "data" / "public_safety_demo" / "incident_identity_view.csv")
    assert {row["incident_id"] for row in public_safety_rows} == {"CAD-INC-1", "RMS-INC-2"}


def test_resolve_batch_manifest_supports_object_storage_memory_uris(tmp_path: Path) -> None:
    _write_memory_csv(
        "memory://identity-ingest/agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_memory_parquet(
        "memory://identity-ingest/agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    manifest_path = _write_manifest(
        tmp_path / "memory-manifest.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            landing_zone_kind="object_storage",
            base_location_key="base_uri",
            base_location_value="memory://identity-ingest",
        ),
    )

    resolved = resolve_batch_manifest(manifest_path)

    assert resolved.input_paths == (
        "memory://identity-ingest/agency_a.csv",
        "memory://identity-ingest/agency_b.parquet",
    )
    assert len(resolved.all_rows()) == 2


def test_run_all_supports_object_storage_manifest_inputs(tmp_path: Path) -> None:
    _write_memory_csv(
        "memory://identity-run/agency_a.csv",
        [_person_row(source_record_id="A-1", person_entity_id="P-1", source_system="source_a")],
    )
    _write_memory_parquet(
        "memory://identity-run/agency_b.parquet",
        [_person_row(source_record_id="B-1", person_entity_id="P-1", source_system="source_b")],
    )
    manifest_path = _write_manifest(
        tmp_path / "memory-run-manifest.yml",
        _manifest_body(
            source_a_path="agency_a.csv",
            source_b_path="agency_b.parquet",
            landing_zone_kind="object_storage",
            base_location_key="base_uri",
            base_location_value="memory://identity-run",
        ),
    )
    base_dir = tmp_path / "object-run"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--manifest",
                str(manifest_path),
            ]
        )
        == 0
    )

    normalized_rows = _read_csv(base_dir / "data" / "normalized" / "normalized_person_records.csv")
    assert {row["source_record_id"] for row in normalized_rows} == {"A-1", "B-1"}
    assert (base_dir / "data" / "golden" / "golden_person_records.csv").exists()
