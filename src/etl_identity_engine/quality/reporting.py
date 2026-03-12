"""Output report helpers."""

from __future__ import annotations

from pathlib import Path


def write_summary_report(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

