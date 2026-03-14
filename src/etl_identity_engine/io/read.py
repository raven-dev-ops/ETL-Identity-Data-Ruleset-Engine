"""Read helpers."""

from __future__ import annotations

import csv
from pathlib import Path


def _require_input_path(path: Path, *, missing_ok: bool) -> bool:
    if path.exists():
        return True
    if missing_ok:
        return False
    raise FileNotFoundError(f"Input file not found: {path}")


def read_csv_dicts(path: Path, *, missing_ok: bool = False) -> list[dict[str, str]]:
    if not _require_input_path(path, missing_ok=missing_ok):
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def read_csv_fieldnames(path: Path, *, missing_ok: bool = False) -> tuple[str, ...]:
    if not _require_input_path(path, missing_ok=missing_ok):
        return ()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return tuple(reader.fieldnames or ())


def read_parquet_dicts(path: Path, *, missing_ok: bool = False) -> list[dict[str, str]]:
    if not _require_input_path(path, missing_ok=missing_ok):
        return []

    try:
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Parquet input requires `pyarrow`. Install project dependencies or use CSV input."
        ) from exc

    table = pq.read_table(path)
    return [
        {
            key: "" if value is None else str(value)
            for key, value in row.items()
        }
        for row in table.to_pylist()
    ]


def read_parquet_fieldnames(path: Path, *, missing_ok: bool = False) -> tuple[str, ...]:
    if not _require_input_path(path, missing_ok=missing_ok):
        return ()

    try:
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Parquet input requires `pyarrow`. Install project dependencies or use CSV input."
        ) from exc

    table = pq.read_table(path)
    return tuple(table.column_names)


def read_dict_rows(path: Path, *, missing_ok: bool = False) -> list[dict[str, str]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_dicts(path, missing_ok=missing_ok)
    if suffix == ".parquet":
        return read_parquet_dicts(path, missing_ok=missing_ok)
    raise ValueError(f"Unsupported input format for {path}: {path.suffix or '<none>'}")


def read_dict_fieldnames(path: Path, *, missing_ok: bool = False) -> tuple[str, ...]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_fieldnames(path, missing_ok=missing_ok)
    if suffix == ".parquet":
        return read_parquet_fieldnames(path, missing_ok=missing_ok)
    raise ValueError(f"Unsupported input format for {path}: {path.suffix or '<none>'}")

