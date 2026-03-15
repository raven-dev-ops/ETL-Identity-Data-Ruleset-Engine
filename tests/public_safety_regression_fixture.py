from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

from etl_identity_engine.normalize.addresses import normalize_address
from etl_identity_engine.normalize.dates import normalize_date
from etl_identity_engine.normalize.names import normalize_name
from etl_identity_engine.normalize.phones import normalize_phone


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "public_safety_regressions"


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def copy_fixture_tree(target_dir: Path) -> Path:
    shutil.copytree(FIXTURE_ROOT, target_dir)
    return target_dir / "manifest.yml"


def load_scenario_expectations() -> dict[str, object]:
    return json.loads((FIXTURE_ROOT / "scenario_expectations.json").read_text(encoding="utf-8"))


def load_landing_rows() -> dict[str, dict[str, str]]:
    rows: list[dict[str, str]] = []
    rows.extend(_read_csv_rows(FIXTURE_ROOT / "landing" / "source_a.csv"))
    rows.extend(_read_csv_rows(FIXTURE_ROOT / "landing" / "source_b.csv"))
    return {
        str(row.get("source_record_id", "")).strip(): row
        for row in rows
        if str(row.get("source_record_id", "")).strip()
    }


def load_normalized_landing_rows() -> dict[str, dict[str, str]]:
    normalized_rows: dict[str, dict[str, str]] = {}
    for source_record_id, row in load_landing_rows().items():
        normalized_rows[source_record_id] = {
            **row,
            "canonical_name": normalize_name(
                " ".join(
                    part for part in (row.get("first_name", "").strip(), row.get("last_name", "").strip()) if part
                )
            ),
            "canonical_dob": normalize_date(row.get("dob", "")) or "",
            "canonical_address": normalize_address(row.get("address", "")),
            "canonical_phone": normalize_phone(row.get("phone", "")),
        }
    return normalized_rows
