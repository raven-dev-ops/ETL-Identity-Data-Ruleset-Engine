from __future__ import annotations

from pathlib import Path

import pytest

from etl_identity_engine.io.read import read_csv_dicts, read_dict_rows, read_parquet_dicts


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_read_csv_dicts_supports_missing_ok(tmp_path: Path) -> None:
    assert read_csv_dicts(tmp_path / "missing.csv", missing_ok=True) == []


def test_read_parquet_dicts_reads_rows_as_strings(tmp_path: Path) -> None:
    path = tmp_path / "rows.parquet"
    _write_parquet(
        path,
        [
            {"source_record_id": "A-1", "score": 1, "note": None},
            {"source_record_id": "A-2", "score": 2, "note": "ok"},
        ],
    )

    rows = read_parquet_dicts(path)

    assert rows == [
        {"source_record_id": "A-1", "score": "1", "note": ""},
        {"source_record_id": "A-2", "score": "2", "note": "ok"},
    ]


def test_read_dict_rows_dispatches_by_suffix(tmp_path: Path) -> None:
    csv_path = tmp_path / "rows.csv"
    _write_text(csv_path, "source_record_id,value\nA-1,alpha\n")

    parquet_path = tmp_path / "rows.parquet"
    _write_parquet(parquet_path, [{"source_record_id": "B-1", "value": "beta"}])

    assert read_dict_rows(csv_path) == [{"source_record_id": "A-1", "value": "alpha"}]
    assert read_dict_rows(parquet_path) == [{"source_record_id": "B-1", "value": "beta"}]


def test_read_dict_rows_rejects_unsupported_suffix(tmp_path: Path) -> None:
    path = tmp_path / "rows.json"
    _write_text(path, '{"source_record_id": "A-1"}')

    with pytest.raises(ValueError, match=r"Unsupported input format"):
        read_dict_rows(path)
