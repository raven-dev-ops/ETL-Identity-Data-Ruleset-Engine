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


def _write_contract_manifest(bundle_dir: Path, *, contract_name: str, files: dict[str, str]) -> Path:
    marker_path = bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER
    marker_path.write_text(
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
    return marker_path


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
