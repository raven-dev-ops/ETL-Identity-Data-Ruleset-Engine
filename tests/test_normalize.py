import csv
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.normalize.addresses import normalize_address
from etl_identity_engine.normalize.dates import normalize_date
from etl_identity_engine.normalize.names import normalize_name
from etl_identity_engine.normalize.phones import normalize_phone


def test_normalize_name_handles_punctuation_and_case() -> None:
    assert normalize_name("Smith, John A.") == "JOHN A SMITH"


def test_normalize_date_parses_common_format() -> None:
    assert normalize_date("03/12/1985") == "1985-03-12"


def test_normalize_address_expands_suffix() -> None:
    assert normalize_address("123 Main St.") == "123 MAIN STREET"


def test_normalize_phone_strips_non_digits() -> None:
    assert normalize_phone("(555) 123-4567") == "5551234567"


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_parquet(path: Path, rows: list[dict[str, str]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_normalize_cli_combines_discovered_source_inputs(tmp_path: Path) -> None:
    input_dir = tmp_path / "synthetic_sources"
    _write_csv(
        input_dir / "person_source_a.csv",
        [
            {
                "source_record_id": "A-1",
                "person_entity_id": "P-1",
                "source_system": "source_a",
                "first_name": "John",
                "last_name": "Smith",
                "dob": "1985-03-12",
                "address": "123 Main St.",
                "phone": "(555) 123-4567",
            }
        ],
    )
    _write_csv(
        input_dir / "person_source_b.csv",
        [
            {
                "source_record_id": "B-1",
                "person_entity_id": "P-1",
                "source_system": "source_b",
                "first_name": "Smith, John",
                "last_name": "",
                "dob": "03/12/1985",
                "address": "123 Main Street",
                "phone": "5551234567",
            }
        ],
    )

    output_path = tmp_path / "normalized" / "normalized_person_records.csv"

    assert (
        main(
            [
                "normalize",
                "--input-dir",
                str(input_dir),
                "--output",
                str(output_path),
            ]
        )
        == 0
    )

    rows = _read_csv(output_path)

    assert len(rows) == 2
    assert {row["source_record_id"] for row in rows} == {"A-1", "B-1"}
    assert {"canonical_name", "canonical_dob", "canonical_address", "canonical_phone"} <= set(rows[0])


def test_normalize_cli_discovers_parquet_inputs_when_csv_is_absent(tmp_path: Path) -> None:
    input_dir = tmp_path / "synthetic_sources"
    _write_parquet(
        input_dir / "person_source_a.parquet",
        [
            {
                "source_record_id": "A-1",
                "person_entity_id": "P-1",
                "source_system": "source_a",
                "first_name": "John",
                "last_name": "Smith",
                "dob": "1985-03-12",
                "address": "123 Main St.",
                "phone": "(555) 123-4567",
            }
        ],
    )
    _write_parquet(
        input_dir / "person_source_b.parquet",
        [
            {
                "source_record_id": "B-1",
                "person_entity_id": "P-1",
                "source_system": "source_b",
                "first_name": "Smith, John",
                "last_name": "",
                "dob": "03/12/1985",
                "address": "123 Main Street",
                "phone": "5551234567",
            }
        ],
    )

    output_path = tmp_path / "normalized" / "normalized_person_records.csv"

    assert (
        main(
            [
                "normalize",
                "--input-dir",
                str(input_dir),
                "--output",
                str(output_path),
            ]
        )
        == 0
    )

    rows = _read_csv(output_path)

    assert len(rows) == 2
    assert {row["source_record_id"] for row in rows} == {"A-1", "B-1"}


def test_normalize_cli_requires_discoverable_inputs_when_no_explicit_files_are_passed(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "normalized" / "normalized_person_records.csv"

    with pytest.raises(
        FileNotFoundError,
        match=r"No normalization input files found",
    ):
        main(
            [
                "normalize",
                "--input-dir",
                str(tmp_path / "missing_sources"),
                "--output",
                str(output_path),
            ]
        )

