from __future__ import annotations

import csv
from io import BytesIO
from pathlib import Path

import pytest
import yaml

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
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
