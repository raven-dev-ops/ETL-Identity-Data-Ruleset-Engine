from __future__ import annotations

import csv
from io import BytesIO
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
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
