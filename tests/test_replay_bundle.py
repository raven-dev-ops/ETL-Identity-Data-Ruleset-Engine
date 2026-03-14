from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path

import pytest

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.ingest.manifest import resolve_batch_manifest
from etl_identity_engine.ingest.replay_bundle import archive_replay_bundle


def _write_manifest(path: Path) -> Path:
    required_columns = "\n".join(f"        - {column}" for column in PERSON_HEADERS)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""
manifest_version: "1.0"
entity_type: person
batch_id: replay-bundle-001
landing_zone:
  kind: object_storage
  base_uri: memory://landing
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
