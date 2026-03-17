from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path
import shutil

import pytest
import yaml

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.ingest.manifest import resolve_batch_manifest
from etl_identity_engine.ingest.public_safety_contracts import (
    CAD_CALL_FOR_SERVICE_CONTRACT,
    PUBLIC_SAFETY_CONTRACT_MARKER,
    RMS_REPORT_PERSON_CONTRACT,
)
from etl_identity_engine.ingest.replay_bundle import archive_replay_bundle


def _write_manifest(
    path: Path,
    *,
    landing_zone_kind: str = "object_storage",
    base_location_key: str = "base_uri",
    base_location_value: str = "memory://landing",
    source_bundles_block: str = "",
) -> Path:
    required_columns = "\n".join(f"        - {column}" for column in PERSON_HEADERS)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""
manifest_version: "1.0"
entity_type: person
batch_id: replay-bundle-001
landing_zone:
  kind: {landing_zone_kind}
  {base_location_key}: {base_location_value}
sources:
  - source_id: source_a
    path: agency_a.csv
    format: csv
    schema_version: person-v1
    required_columns:
{required_columns}
  - source_id: source_b
    path: agency_b.parquet
    format: parquet
    schema_version: person-v1
    required_columns:
{required_columns}
{source_bundles_block}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _person_row(
    *,
    source_record_id: str,
    person_entity_id: str,
    source_system: str,
    first_name: str,
    last_name: str,
    dob: str,
    address: str,
    phone: str,
) -> dict[str, str]:
    return {
        "source_record_id": source_record_id,
        "person_entity_id": person_entity_id,
        "source_system": source_system,
        "first_name": first_name,
        "last_name": last_name,
        "dob": dob,
        "address": address,
        "city": "Columbus",
        "state": "OH",
        "postal_code": "43004",
        "phone": phone,
        "updated_at": "2025-01-01T00:00:00Z",
        "is_conflict_variant": "false",
        "conflict_types": "",
    }


def _write_csv_payload(rows: list[dict[str, str]]) -> bytes:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _write_csv_file(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_write_csv_payload(rows))


def _write_remote_bytes(uri: str, payload: bytes) -> None:
    fsspec = pytest.importorskip("fsspec")

    with fsspec.open(uri, "wb") as handle:
        handle.write(payload)


def _write_remote_csv(uri: str, rows: list[dict[str, str]]) -> None:
    _write_remote_bytes(uri, _write_csv_payload(rows))


def _write_parquet_file(path: Path, rows: list[dict[str, str]]) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


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


def _write_contract_manifest(bundle_dir: Path, payload: dict[str, object]) -> None:
    (bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER).write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _build_cad_vendor_profile_bundle(root_dir: Path) -> Path:
    root_dir.mkdir(parents=True, exist_ok=True)
    _write_csv_file(
        root_dir / "vendor_person_records.csv",
        [
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
        ],
    )
    _write_csv_file(
        root_dir / "vendor_incident_records.csv",
        [
            {
                "cad_event_key": "CAD-INC-1",
                "call_received_at": "2026-03-14T12:00:00Z",
                "street_address": "100 WEST MAIN STREET",
                "city_name": "Columbus",
                "state_code": "OH",
            }
        ],
    )
    _write_csv_file(
        root_dir / "vendor_incident_person_links.csv",
        [
            {
                "cad_link_key": "CAD-LINK-1",
                "cad_event_key": "CAD-INC-1",
                "master_name_id": "PS-1",
                "cad_person_key": "CAD-1",
                "party_role": "REPORTING_PARTY",
            }
        ],
    )
    _write_contract_manifest(
        root_dir,
        {
            "contract_name": CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
            "contract_version": "v1",
            "files": {
                "person_records": "vendor_person_records.csv",
                "incident_records": "vendor_incident_records.csv",
                "incident_person_links": "vendor_incident_person_links.csv",
            },
        },
    )
    return root_dir


def _build_rms_overlay_bundle(root_dir: Path) -> Path:
    root_dir.mkdir(parents=True, exist_ok=True)
    _write_csv_file(
        root_dir / "vendor_person_records.csv",
        [
            {
                "vendor_person_record_key": "RMS-1",
                "vendor_master_person_key": "PS-10",
                "given_name": "Jordan",
                "surname": "Mills",
                "date_of_birth": "1985-03-12",
                "street_line": "700 WEST TOWN STREET",
                "municipality": "Columbus",
                "region_code": "OH",
                "zip_code": "43004",
                "contact_phone": "(555) 123-4567",
                "record_last_updated_at": "2026-03-14T00:00:00Z",
                "variant_flag": "false",
                "variant_codes": "",
            }
        ],
    )
    _write_csv_file(
        root_dir / "vendor_incident_records.csv",
        [
            {
                "vendor_incident_key": "RMS-INC-1",
                "reported_at": "2026-03-14T12:00:00Z",
                "location_text": "700 WEST TOWN STREET",
                "municipality": "Columbus",
                "region_code": "OH",
            }
        ],
    )
    _write_csv_file(
        root_dir / "vendor_incident_person_links.csv",
        [
            {
                "vendor_link_key": "RMS-LINK-1",
                "vendor_incident_key": "RMS-INC-1",
                "vendor_master_person_key": "PS-10",
                "vendor_person_record_key": "RMS-1",
                "involvement_role": "WITNESS",
            }
        ],
    )
    _write_mapping_overlay(
        root_dir,
        relative_path="overlays/vendor_columns.yml",
        contract_name=RMS_REPORT_PERSON_CONTRACT.contract_name,
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
                "defaults": {"source_system": "rms"},
            },
            "incident_records": {
                "columns": {
                    "incident_id": "vendor_incident_key",
                    "occurred_at": "reported_at",
                    "location": "location_text",
                    "city": "municipality",
                    "state": "region_code",
                },
                "defaults": {"source_system": "rms"},
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
    _write_contract_manifest(
        root_dir,
        {
            "contract_name": RMS_REPORT_PERSON_CONTRACT.contract_name,
            "contract_version": "v1",
            "mapping_overlay": "overlays/vendor_columns.yml",
            "files": {
                "person_records": "vendor_person_records.csv",
                "incident_records": "vendor_incident_records.csv",
                "incident_person_links": "vendor_incident_person_links.csv",
            },
        },
    )
    return root_dir


def test_archive_replay_bundle_supports_object_storage_manifest_sources(tmp_path: Path) -> None:
    fsspec = pytest.importorskip("fsspec")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    manifest_path = _write_manifest(tmp_path / "manifest.yml")
    source_a_rows = [
        _person_row(
            source_record_id="A-1",
            person_entity_id="P-1",
            source_system="source_a",
            first_name="John",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        )
    ]
    source_b_rows = [
        _person_row(
            source_record_id="B-1",
            person_entity_id="P-2",
            source_system="source_b",
            first_name="Jon",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        )
    ]

    source_a_uri = "memory://landing/agency_a.csv"
    source_b_uri = "memory://landing/agency_b.parquet"
    with fsspec.open(source_a_uri, "wb") as handle:
        handle.write(_write_csv_payload(source_a_rows))
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(source_b_rows), parquet_buffer)
    with fsspec.open(source_b_uri, "wb") as handle:
        handle.write(parquet_buffer.getvalue().to_pybytes())

    resolved_manifest = resolve_batch_manifest(manifest_path)
    verification = archive_replay_bundle(
        run_id="run-archive-001",
        base_dir=tmp_path / "run",
        resolved_manifest=resolved_manifest,
        created_at_utc="2026-03-14T00:00:00Z",
    )

    assert verification.status == "verified"
    assert verification.recoverable is True
    assert verification.source_count == 2
    assert verification.bundle_manifest_path.exists()
    assert (verification.landing_snapshot_root / "agency_a.csv").exists()
    assert (verification.landing_snapshot_root / "agency_b.parquet").exists()
    assert verification.replay_manifest_path.exists()


def test_archive_replay_bundle_supports_source_bundles_in_replay_manifest(tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        landing_zone_kind="local_filesystem",
        base_location_key="base_path",
        base_location_value="./landing",
        source_bundles_block="""
source_bundles:
  - bundle_id: cad_primary
    source_class: cad
    path: cad_vendor_profile_bundle
    contract_name: cad_call_for_service
    contract_version: v1
    vendor_profile: cad_county_dispatch_v1
  - bundle_id: rms_primary
    source_class: rms
    path: rms_vendor_bundle
    contract_name: rms_report_person
    contract_version: v1
""",
    )
    _write_csv_file(
        landing_root / "agency_a.csv",
        [
            _person_row(
                source_record_id="A-1",
                person_entity_id="P-1",
                source_system="source_a",
                first_name="John",
                last_name="Smith",
                dob="1985-03-12",
                address="123 Main St",
                phone="5551111111",
            )
        ],
    )
    _write_parquet_file(
        landing_root / "agency_b.parquet",
        [
            _person_row(
                source_record_id="B-1",
                person_entity_id="P-2",
                source_system="source_b",
                first_name="Jon",
                last_name="Smith",
                dob="1985-03-12",
                address="123 Main St",
                phone="5551111111",
            )
        ],
    )
    _build_cad_vendor_profile_bundle(landing_root / "cad_vendor_profile_bundle")
    _build_rms_overlay_bundle(landing_root / "rms_vendor_bundle")

    resolved_manifest = resolve_batch_manifest(manifest_path)

    verification = archive_replay_bundle(
        run_id="run-archive-bundles-001",
        base_dir=tmp_path / "run",
        resolved_manifest=resolved_manifest,
        created_at_utc="2026-03-16T00:00:00Z",
    )

    assert verification.status == "verified"
    assert verification.recoverable is True
    assert (verification.landing_snapshot_root / "cad_vendor_profile_bundle" / PUBLIC_SAFETY_CONTRACT_MARKER).exists()
    assert (
        verification.landing_snapshot_root / "rms_vendor_bundle" / "overlays" / "vendor_columns.yml"
    ).exists()

    manifest_path.unlink()
    shutil.rmtree(landing_root)

    replayed_manifest = resolve_batch_manifest(verification.replay_manifest_path)

    assert len(replayed_manifest.sources) == 2
    assert len(replayed_manifest.source_bundles) == 2
    cad_bundle = next(bundle for bundle in replayed_manifest.source_bundles if bundle.spec.bundle_id == "cad_primary")
    rms_bundle = next(bundle for bundle in replayed_manifest.source_bundles if bundle.spec.bundle_id == "rms_primary")
    assert cad_bundle.vendor_profile == "cad_county_dispatch_v1"
    assert rms_bundle.mapping_overlay_reference is not None
    assert rms_bundle.files[0].rows[0]["source_record_id"] == "RMS-1"


def test_archive_replay_bundle_supports_object_storage_source_bundles(tmp_path: Path) -> None:
    fsspec = pytest.importorskip("fsspec")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        base_location_value="memory://replay-source-bundles-001",
        source_bundles_block="""
source_bundles:
  - bundle_id: cad_primary
    source_class: cad
    path: cad_vendor_profile_bundle
    contract_name: cad_call_for_service
    contract_version: v1
    vendor_profile: cad_county_dispatch_v1
  - bundle_id: rms_primary
    source_class: rms
    path: rms_vendor_bundle
    contract_name: rms_report_person
    contract_version: v1
""",
    )
    landing_root = "memory://replay-source-bundles-001"

    source_a_rows = [
        _person_row(
            source_record_id="A-1",
            person_entity_id="P-1",
            source_system="source_a",
            first_name="John",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        )
    ]
    source_b_rows = [
        _person_row(
            source_record_id="B-1",
            person_entity_id="P-2",
            source_system="source_b",
            first_name="Jon",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        )
    ]

    _write_remote_csv(f"{landing_root}/agency_a.csv", source_a_rows)
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(source_b_rows), parquet_buffer)
    _write_remote_bytes(f"{landing_root}/agency_b.parquet", parquet_buffer.getvalue().to_pybytes())

    _write_remote_csv(
        f"{landing_root}/cad_vendor_profile_bundle/vendor_person_records.csv",
        [
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
        ],
    )
    _write_remote_csv(
        f"{landing_root}/cad_vendor_profile_bundle/vendor_incident_records.csv",
        [
            {
                "cad_event_key": "CAD-INC-1",
                "call_received_at": "2026-03-14T12:00:00Z",
                "street_address": "100 WEST MAIN STREET",
                "city_name": "Columbus",
                "state_code": "OH",
            }
        ],
    )
    _write_remote_csv(
        f"{landing_root}/cad_vendor_profile_bundle/vendor_incident_person_links.csv",
        [
            {
                "cad_link_key": "CAD-LINK-1",
                "cad_event_key": "CAD-INC-1",
                "master_name_id": "PS-1",
                "cad_person_key": "CAD-1",
                "party_role": "REPORTING_PARTY",
            }
        ],
    )
    _write_remote_bytes(
        f"{landing_root}/cad_vendor_profile_bundle/{PUBLIC_SAFETY_CONTRACT_MARKER}",
        yaml.safe_dump(
            {
                "contract_name": CAD_CALL_FOR_SERVICE_CONTRACT.contract_name,
                "contract_version": "v1",
                "files": {
                    "person_records": "vendor_person_records.csv",
                    "incident_records": "vendor_incident_records.csv",
                    "incident_person_links": "vendor_incident_person_links.csv",
                },
            },
            sort_keys=False,
        ).encode("utf-8"),
    )

    _write_remote_csv(
        f"{landing_root}/rms_vendor_bundle/vendor_person_records.csv",
        [
            {
                "vendor_person_record_key": "RMS-1",
                "vendor_master_person_key": "PS-10",
                "given_name": "Jordan",
                "surname": "Mills",
                "date_of_birth": "1985-03-12",
                "street_line": "700 WEST TOWN STREET",
                "municipality": "Columbus",
                "region_code": "OH",
                "zip_code": "43004",
                "contact_phone": "(555) 123-4567",
                "record_last_updated_at": "2026-03-14T00:00:00Z",
                "variant_flag": "false",
                "variant_codes": "",
            }
        ],
    )
    _write_remote_csv(
        f"{landing_root}/rms_vendor_bundle/vendor_incident_records.csv",
        [
            {
                "vendor_incident_key": "RMS-INC-1",
                "reported_at": "2026-03-14T12:00:00Z",
                "location_text": "700 WEST TOWN STREET",
                "municipality": "Columbus",
                "region_code": "OH",
            }
        ],
    )
    _write_remote_csv(
        f"{landing_root}/rms_vendor_bundle/vendor_incident_person_links.csv",
        [
            {
                "vendor_link_key": "RMS-LINK-1",
                "vendor_incident_key": "RMS-INC-1",
                "vendor_master_person_key": "PS-10",
                "vendor_person_record_key": "RMS-1",
                "involvement_role": "WITNESS",
            }
        ],
    )
    _write_remote_bytes(
        f"{landing_root}/rms_vendor_bundle/overlays/vendor_columns.yml",
        yaml.safe_dump(
            {
                "overlay_version": "v1",
                "contract_name": RMS_REPORT_PERSON_CONTRACT.contract_name,
                "contract_version": "v1",
                "files": {
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
                        "defaults": {"source_system": "rms"},
                    },
                    "incident_records": {
                        "columns": {
                            "incident_id": "vendor_incident_key",
                            "occurred_at": "reported_at",
                            "location": "location_text",
                            "city": "municipality",
                            "state": "region_code",
                        },
                        "defaults": {"source_system": "rms"},
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
            },
            sort_keys=False,
        ).encode("utf-8"),
    )
    _write_remote_bytes(
        f"{landing_root}/rms_vendor_bundle/{PUBLIC_SAFETY_CONTRACT_MARKER}",
        yaml.safe_dump(
            {
                "contract_name": RMS_REPORT_PERSON_CONTRACT.contract_name,
                "contract_version": "v1",
                "mapping_overlay": "overlays/vendor_columns.yml",
                "files": {
                    "person_records": "vendor_person_records.csv",
                    "incident_records": "vendor_incident_records.csv",
                    "incident_person_links": "vendor_incident_person_links.csv",
                },
            },
            sort_keys=False,
        ).encode("utf-8"),
    )

    resolved_manifest = resolve_batch_manifest(manifest_path)

    verification = archive_replay_bundle(
        run_id="run-archive-object-bundles-001",
        base_dir=tmp_path / "run",
        resolved_manifest=resolved_manifest,
        created_at_utc="2026-03-16T00:00:00Z",
    )

    assert verification.status == "verified"
    assert verification.recoverable is True
    assert (verification.landing_snapshot_root / "cad_vendor_profile_bundle" / PUBLIC_SAFETY_CONTRACT_MARKER).exists()
    assert (
        verification.landing_snapshot_root / "rms_vendor_bundle" / "overlays" / "vendor_columns.yml"
    ).exists()

    fsspec.filesystem("memory").rm("/replay-source-bundles-001", recursive=True)

    replayed_manifest = resolve_batch_manifest(verification.replay_manifest_path)

    assert len(replayed_manifest.sources) == 2
    assert len(replayed_manifest.source_bundles) == 2
    cad_bundle = next(bundle for bundle in replayed_manifest.source_bundles if bundle.spec.bundle_id == "cad_primary")
    rms_bundle = next(bundle for bundle in replayed_manifest.source_bundles if bundle.spec.bundle_id == "rms_primary")
    assert cad_bundle.vendor_profile == "cad_county_dispatch_v1"
    assert rms_bundle.mapping_overlay_reference is not None
    assert rms_bundle.files[0].rows[0]["source_record_id"] == "RMS-1"
