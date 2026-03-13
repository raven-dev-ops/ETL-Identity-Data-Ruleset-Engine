import csv
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.runtime_config import load_pipeline_config


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.strip() + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_valid_config(config_dir: Path, *, thresholds: str | None = None) -> None:
    _write_text(
        config_dir / "normalization_rules.yml",
        """
name_normalization:
  trim_whitespace: true
  remove_punctuation: false
  uppercase: false
date_normalization:
  accepted_formats:
    - "%Y-%m-%d"
  output_format: "%Y/%m/%d"
phone_normalization:
  digits_only: true
""",
    )
    _write_text(
        config_dir / "blocking_rules.yml",
        """
blocking_passes:
  - name: birth_year_only
    fields:
      - birth_year
""",
    )
    _write_text(
        config_dir / "matching_rules.yml",
        """
weights:
  canonical_name: 0.5
  canonical_dob: 0.3
  canonical_phone: 0.1
  canonical_address: 0.1
""",
    )
    _write_text(
        config_dir / "thresholds.yml",
        thresholds
        or """
thresholds:
  auto_merge: 0.95
  manual_review_min: 0.5
  no_match_max: 0.49
""",
    )
    _write_text(
        config_dir / "survivorship_rules.yml",
        """
source_priority:
  - source_b
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
""",
    )


def test_load_pipeline_config_reads_custom_directory(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)

    config = load_pipeline_config(config_dir)

    assert config.normalization.name.remove_punctuation is False
    assert config.normalization.name.uppercase is False
    assert config.normalization.date.output_format == "%Y/%m/%d"
    assert [blocking_pass.fields for blocking_pass in config.matching.blocking_passes] == [("birth_year",)]
    assert config.matching.weights == {
        "canonical_name": 0.5,
        "canonical_dob": 0.3,
        "canonical_phone": 0.1,
        "canonical_address": 0.1,
    }
    assert config.survivorship.source_priority == ("source_b", "source_a")


def test_cli_commands_respect_custom_config_dir(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_text(
        config_dir / "normalization_rules.yml",
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
""",
    )
    _write_text(
        config_dir / "blocking_rules.yml",
        """
blocking_passes:
  - name: last_name_birth_year
    fields:
      - last_name
      - birth_year
""",
    )
    _write_text(
        config_dir / "matching_rules.yml",
        """
weights:
  canonical_name: 1.0
  canonical_dob: 0.0
  canonical_phone: 0.0
  canonical_address: 0.0
""",
    )
    _write_text(
        config_dir / "thresholds.yml",
        """
thresholds:
  auto_merge: 0.95
  manual_review_min: 0.5
  no_match_max: 0.49
""",
    )
    _write_text(
        config_dir / "survivorship_rules.yml",
        """
source_priority:
  - source_b
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
""",
    )

    normalized_input = tmp_path / "normalized.csv"
    _write_csv(
        normalized_input,
        [
            {
                "source_record_id": "A-1",
                "person_entity_id": "P-1",
                "source_system": "source_a",
                "first_name": "JOHN",
                "last_name": "SMITH",
                "dob": "1985-03-12",
                "address": "123 MAIN ST",
                "phone": "5551234567",
                "canonical_name": "JOHN SMITH",
                "canonical_dob": "1985-03-12",
                "canonical_address": "123 MAIN ST",
                "canonical_phone": "5551234567",
            },
            {
                "source_record_id": "B-1",
                "person_entity_id": "P-1",
                "source_system": "source_b",
                "first_name": "JONATHAN",
                "last_name": "SMITH",
                "dob": "1985-04-12",
                "address": "123 MAIN STREET",
                "phone": "5551230000",
                "canonical_name": "JOHN SMITH",
                "canonical_dob": "1985-04-12",
                "canonical_address": "123 MAIN STREET",
                "canonical_phone": "5551230000",
            },
        ],
    )

    match_output = tmp_path / "candidate_scores.csv"
    golden_output = tmp_path / "golden.csv"

    assert (
        main(
            [
                "match",
                "--input",
                str(normalized_input),
                "--output",
                str(match_output),
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "golden",
                "--input",
                str(normalized_input),
                "--output",
                str(golden_output),
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )

    match_rows = _read_csv(match_output)
    golden_rows = _read_csv(golden_output)

    assert len(match_rows) == 1
    assert match_rows[0]["score"] == "1.0"
    assert match_rows[0]["decision"] == "auto_merge"
    assert golden_rows[0]["first_name"] == "JONATHAN"


def test_load_pipeline_config_rejects_missing_required_sections(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "thresholds.yml",
        """
not_thresholds:
  auto_merge: 0.95
""",
    )

    with pytest.raises(ValueError, match=r"thresholds\.yml: top-level config contains unsupported keys: not_thresholds"):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_unsupported_blocking_fields(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "blocking_rules.yml",
        """
blocking_passes:
  - name: bad_block
    fields:
      - nickname
""",
    )

    with pytest.raises(ValueError, match=r"blocking_rules\.yml: blocking_passes\[0\]\.fields contains unsupported values: nickname"):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_inconsistent_thresholds(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(
        config_dir,
        thresholds="""
thresholds:
  auto_merge: 1.1
  manual_review_min: 0.7
  no_match_max: 0.7
""",
    )

    with pytest.raises(
        ValueError,
        match=r"thresholds\.yml: thresholds\.no_match_max must be less than thresholds\.manual_review_min",
    ):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_auto_merge_threshold_above_total_weight(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(
        config_dir,
        thresholds="""
thresholds:
  auto_merge: 1.1
  manual_review_min: 0.7
  no_match_max: 0.6
""",
    )

    with pytest.raises(
        ValueError,
        match=r"thresholds\.yml: thresholds\.auto_merge cannot exceed the total configured match weight",
    ):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_missing_weight_fields(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "matching_rules.yml",
        """
weights:
  canonical_name: 1.0
  canonical_dob: 0.0
  canonical_phone: 0.0
""",
    )

    with pytest.raises(
        ValueError,
        match=r"matching_rules\.yml: weights is missing required fields: canonical_address",
    ):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_invalid_survivorship_rules(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "survivorship_rules.yml",
        """
source_priority:
  - source_a
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
    strategy: newest_value
""",
    )

    with pytest.raises(ValueError, match=r"survivorship_rules\.yml: source_priority contains duplicate source names"):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_unsupported_survivorship_strategy(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "survivorship_rules.yml",
        """
source_priority:
  - source_a
  - source_b
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
    strategy: newest_value
""",
    )

    with pytest.raises(
        ValueError,
        match=r"survivorship_rules\.yml: field_rules\.phone\.strategy must be one of: source_priority_then_non_null",
    ):
        load_pipeline_config(config_dir)
