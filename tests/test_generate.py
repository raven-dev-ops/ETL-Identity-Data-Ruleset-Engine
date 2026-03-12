import csv
from pathlib import Path

from etl_identity_engine.generate.synth_generator import generate_synthetic_sources


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_generate_synthetic_sources_is_deterministic(tmp_path: Path) -> None:
    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    result1 = generate_synthetic_sources(
        out1,
        profile="small",
        seed=42,
        duplicate_rate=0.4,
        formats=("csv",),
    )
    result2 = generate_synthetic_sources(
        out2,
        profile="small",
        seed=42,
        duplicate_rate=0.4,
        formats=("csv",),
    )

    assert result1.summary == result2.summary

    file_names = sorted(path.name for path in result1.generated_files)
    file_names_2 = sorted(path.name for path in result2.generated_files)
    assert file_names == file_names_2

    for file_name in file_names:
        first = (out1 / file_name).read_text(encoding="utf-8")
        second = (out2 / file_name).read_text(encoding="utf-8")
        assert first == second


def test_profile_sizes_and_duplicate_annotations(tmp_path: Path) -> None:
    output = tmp_path / "small"
    result = generate_synthetic_sources(
        output,
        profile="small",
        seed=123,
        duplicate_rate=0.5,
        formats=("csv",),
    )

    assert result.summary["person_entity_count"] == 24
    assert result.summary["source_a_record_count"] == 24
    assert result.summary["source_b_record_count"] == 24
    assert result.summary["duplicate_variant_count"] == 12
    assert result.summary["incident_count"] == 12

    source_b_rows = _read_csv_rows(output / "person_source_b.csv")
    conflict_rows = _read_csv_rows(output / "conflict_annotations.csv")
    flagged = [row for row in source_b_rows if row["is_conflict_variant"] == "true"]

    assert len(flagged) == 12
    assert len(conflict_rows) == 12
    assert all(row["conflict_types"] for row in conflict_rows)


def test_generate_outputs_csv_and_parquet(tmp_path: Path) -> None:
    output = tmp_path / "exports"
    result = generate_synthetic_sources(
        output,
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
    )

    generated_names = {path.name for path in result.generated_files}
    assert "person_source_a.csv" in generated_names
    assert "person_source_a.parquet" in generated_names
    assert "incident_records.csv" in generated_names
    assert "incident_records.parquet" in generated_names
    assert "generation_summary.json" in generated_names


def test_incident_links_reference_valid_records(tmp_path: Path) -> None:
    output = tmp_path / "links"
    generate_synthetic_sources(output, profile="small", seed=99, formats=("csv",))

    source_a = _read_csv_rows(output / "person_source_a.csv")
    source_b = _read_csv_rows(output / "person_source_b.csv")
    links = _read_csv_rows(output / "incident_person_links.csv")

    valid_ids = {row["source_record_id"] for row in source_a + source_b}
    assert links
    assert all(link["source_record_id"] in valid_ids for link in links)

