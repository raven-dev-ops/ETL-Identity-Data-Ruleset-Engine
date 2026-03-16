from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.delivery_publish import publish_delivery_snapshot
from etl_identity_engine.field_authorization import (
    DELIVERY_GOLDEN_RECORDS_SURFACE,
    FieldAuthorizationConfig,
    FieldAuthorizationDenied,
)
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


def _create_completed_run_bundle(tmp_path: Path) -> tuple[Path, str, SQLitePipelineStore]:
    base_dir = tmp_path / "run"
    db_path = tmp_path / "state" / "pipeline.sqlite"

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
    return db_path, run_id, store


def test_publish_delivery_writes_versioned_snapshot_and_current_pointer(tmp_path: Path) -> None:
    db_path, run_id, store = _create_completed_run_bundle(tmp_path)
    publish_root = tmp_path / "published"
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


def test_publish_delivery_masks_configured_fields_without_changing_contract_shape(
    tmp_path: Path,
) -> None:
    db_path, run_id, store = _create_completed_run_bundle(tmp_path)
    publish_root = tmp_path / "masked-delivery"
    bundle = store.load_run_bundle(run_id)
    field_authorization = FieldAuthorizationConfig(
        surface_rules={
            DELIVERY_GOLDEN_RECORDS_SURFACE: {
                "first_name": "mask",
                "phone": "mask",
            }
        }
    )

    published = publish_delivery_snapshot(
        bundle=bundle,
        state_db_path=db_path,
        output_root=publish_root,
        field_authorization=field_authorization,
    )

    golden_rows = _read_csv_rows(published.snapshot_dir / "golden_person_records.csv")
    assert golden_rows[0]["first_name"] == "[MASKED]"
    assert golden_rows[0]["phone"] == "[MASKED]"
    assert golden_rows[0]["last_name"] == bundle.golden_rows[0]["last_name"]
    assert list(golden_rows[0]) == list(bundle.golden_rows[0])

    manifest = json.loads((published.snapshot_dir / "delivery_manifest.json").read_text(encoding="utf-8"))
    assert manifest["row_counts"]["golden_records"] == len(bundle.golden_rows)


def test_publish_delivery_denies_snapshot_publication_when_policy_blocks_surface(
    tmp_path: Path,
) -> None:
    db_path, run_id, store = _create_completed_run_bundle(tmp_path)
    publish_root = tmp_path / "denied-delivery"
    bundle = store.load_run_bundle(run_id)
    field_authorization = FieldAuthorizationConfig(
        surface_rules={DELIVERY_GOLDEN_RECORDS_SURFACE: {"phone": "deny"}}
    )

    with pytest.raises(FieldAuthorizationDenied, match=r"delivery\.golden_records"):
        publish_delivery_snapshot(
            bundle=bundle,
            state_db_path=db_path,
            output_root=publish_root,
            field_authorization=field_authorization,
        )

    assert not publish_root.exists()
