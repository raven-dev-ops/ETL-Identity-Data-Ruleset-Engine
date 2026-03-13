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


def test_normalize_address_canonicalizes_units_and_directionals() -> None:
    assert normalize_address("Apt 5B, 123 n. Main St.") == "123 NORTH MAIN STREET UNIT 5B"


def test_normalize_address_handles_hash_style_unit_markers() -> None:
    assert normalize_address("123 Main St Apt #5B") == "123 MAIN STREET UNIT 5B"


def test_normalize_address_preserves_po_box_shape() -> None:
    assert normalize_address("P.O. Box 12") == "PO BOX 12"


def test_normalize_phone_strips_non_digits() -> None:
    assert normalize_phone("(555) 123-4567") == "5551234567"


def test_normalize_phone_supports_opt_in_e164_output() -> None:
    assert (
        normalize_phone("(555) 123-4567", output_format="e164", default_country_code="1")
        == "+15551234567"
    )


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


def test_normalize_cli_respects_e164_phone_output_config(tmp_path: Path) -> None:
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
                "address": "Apt 5B, 123 n. Main St.",
                "phone": "(555) 123-4567",
            }
        ],
    )

    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "normalization_rules.yml").write_text(
        """
name_normalization:
  trim_whitespace: true
  remove_punctuation: true
  uppercase: true
date_normalization:
  accepted_formats:
    - "%Y-%m-%d"
  output_format: "%Y-%m-%d"
phone_normalization:
  digits_only: true
  output_format: e164
  default_country_code: "1"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (config_dir / "blocking_rules.yml").write_text(
        """
blocking_passes:
  - name: birth_year_only
    fields:
      - birth_year
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (config_dir / "matching_rules.yml").write_text(
        """
weights:
  canonical_name: 0.5
  canonical_dob: 0.3
  canonical_phone: 0.1
  canonical_address: 0.1
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (config_dir / "thresholds.yml").write_text(
        """
thresholds:
  auto_merge: 0.9
  manual_review_min: 0.6
  no_match_max: 0.59
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (config_dir / "survivorship_rules.yml").write_text(
        """
source_priority:
  - source_a
field_rules:
  first_name:
    strategy: source_priority_then_non_null
  last_name:
    strategy: source_priority_then_non_null
  dob:
    strategy: source_priority_then_non_null
  address:
    strategy: source_priority_then_non_null
  phone:
    strategy: source_priority_then_non_null
""".strip()
        + "\n",
        encoding="utf-8",
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
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )

    rows = _read_csv(output_path)

    assert rows[0]["canonical_address"] == "123 NORTH MAIN STREET UNIT 5B"
    assert rows[0]["canonical_phone"] == "+15551234567"


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

