"""Write helpers."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Sequence


def write_csv_dicts(
    path: Path,
    rows: list[dict[str, object]],
    *,
    fieldnames: Sequence[str] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    resolved_fieldnames = list(fieldnames) if fieldnames is not None else []
    if not resolved_fieldnames and rows:
        resolved_fieldnames = list(rows[0].keys())

    if not rows and not resolved_fieldnames:
        path.write_text("", encoding="utf-8")
        return

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=resolved_fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

