from __future__ import annotations

import csv
import json
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.output_contracts import (
    DELIVERY_ARTIFACT_HEADERS,
    DELIVERY_CONTRACT_NAME,
    DELIVERY_CONTRACT_VERSION,
    DELIVERY_CURRENT_POINTER_KEYS,
    DELIVERY_MANIFEST_KEYS,
)
from etl_identity_engine.storage.sqlite_store import SQLitePipelineStore


def _read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return next(csv.reader(handle), [])


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_publish_delivery_writes_versioned_snapshot_and_current_pointer(tmp_path: Path) -> None:
    base_dir = tmp_path / "run"
    db_path = tmp_path / "state" / "pipeline.sqlite"
    publish_root = tmp_path / "published"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    run_id = store.latest_completed_run_id()
    assert run_id is not None
    bundle = store.load_run_bundle(run_id)

    assert (
        main(
            [
                "publish-delivery",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--output-dir",
                str(publish_root),
            ]
        )
        == 0
    )

    contract_root = publish_root / DELIVERY_CONTRACT_NAME / DELIVERY_CONTRACT_VERSION
    snapshot_dir = contract_root / "snapshots" / run_id
    manifest_path = snapshot_dir / "delivery_manifest.json"
    current_pointer_path = contract_root / "current.json"

    assert snapshot_dir.exists()
    assert manifest_path.exists()
    assert current_pointer_path.exists()

    golden_path = snapshot_dir / "golden_person_records.csv"
    crosswalk_path = snapshot_dir / "source_to_golden_crosswalk.csv"

    assert _read_csv_header(golden_path) == list(DELIVERY_ARTIFACT_HEADERS[Path("golden_person_records.csv")])
    assert _read_csv_header(crosswalk_path) == list(
        DELIVERY_ARTIFACT_HEADERS[Path("source_to_golden_crosswalk.csv")]
    )
    assert _read_csv_rows(golden_path) == bundle.golden_rows
    assert _read_csv_rows(crosswalk_path) == bundle.crosswalk_rows

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert set(DELIVERY_MANIFEST_KEYS) <= set(manifest)
    assert manifest["contract_name"] == DELIVERY_CONTRACT_NAME
    assert manifest["contract_version"] == DELIVERY_CONTRACT_VERSION
    assert manifest["snapshot_id"] == run_id
    assert manifest["run_id"] == run_id
    assert manifest["row_counts"]["golden_records"] == len(bundle.golden_rows)
    assert manifest["row_counts"]["source_to_golden_crosswalk"] == len(bundle.crosswalk_rows)
    assert [artifact["name"] for artifact in manifest["artifacts"]] == [
        "golden_records",
        "source_to_golden_crosswalk",
    ]
    assert all(len(artifact["sha256"]) == 64 for artifact in manifest["artifacts"])

    current_pointer = json.loads(current_pointer_path.read_text(encoding="utf-8"))
    assert set(DELIVERY_CURRENT_POINTER_KEYS) <= set(current_pointer)
    assert current_pointer["run_id"] == run_id
    assert current_pointer["snapshot_id"] == run_id
    assert current_pointer["relative_snapshot_path"] == f"snapshots/{run_id}"
    assert current_pointer["relative_manifest_path"] == f"snapshots/{run_id}/delivery_manifest.json"

    assert (
        main(
            [
                "publish-delivery",
                "--state-db",
                str(db_path),
                "--output-dir",
                str(publish_root),
            ]
        )
        == 0
    )
    assert [path.name for path in (contract_root / "snapshots").iterdir() if path.is_dir()] == [run_id]
