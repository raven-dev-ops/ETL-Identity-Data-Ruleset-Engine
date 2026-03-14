"""Stable downstream delivery publication for persisted run outputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

from etl_identity_engine.io.write import write_csv_dicts
from etl_identity_engine.output_contracts import (
    CROSSWALK_HEADERS,
    DELIVERY_ARTIFACT_HEADERS,
    DELIVERY_CONTRACT_NAME,
    DELIVERY_CONTRACT_VERSION,
    DELIVERY_CURRENT_POINTER_KEYS,
    DELIVERY_MANIFEST_KEYS,
    GOLDEN_HEADERS,
)
from etl_identity_engine.storage.sqlite_store import PersistedRunBundle


@dataclass(frozen=True)
class PublishedDeliverySnapshot:
    contract_root: Path
    snapshot_dir: Path
    manifest_path: Path
    current_pointer_path: Path
    snapshot_id: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_atomic_json(path: Path, payload: dict[str, object]) -> None:
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    _write_json(temporary_path, payload)
    os.replace(temporary_path, path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_delivery_manifest(
    *,
    bundle: PersistedRunBundle,
    state_db_path: Path,
    snapshot_id: str,
    contract_version: str,
    published_at_utc: str,
    snapshot_dir: Path,
) -> dict[str, object]:
    artifact_rows = (
        ("golden_records", Path("golden_person_records.csv"), GOLDEN_HEADERS, bundle.golden_rows),
        (
            "source_to_golden_crosswalk",
            Path("source_to_golden_crosswalk.csv"),
            CROSSWALK_HEADERS,
            bundle.crosswalk_rows,
        ),
    )
    artifacts: list[dict[str, object]] = []
    for artifact_name, relative_path, headers, rows in artifact_rows:
        artifacts.append(
            {
                "name": artifact_name,
                "relative_path": str(relative_path).replace("\\", "/"),
                "row_count": len(rows),
                "headers": list(headers),
                "sha256": _sha256_file(snapshot_dir / relative_path),
            }
        )

    manifest = {
        "contract_name": DELIVERY_CONTRACT_NAME,
        "contract_version": contract_version,
        "snapshot_id": snapshot_id,
        "published_at_utc": published_at_utc,
        "run_id": bundle.run.run_id,
        "state_db": str(state_db_path.name),
        "source_run": {
            "run_id": bundle.run.run_id,
            "run_key": bundle.run.run_key,
            "batch_id": bundle.run.batch_id or "",
            "input_mode": bundle.run.input_mode,
            "manifest_path": bundle.run.manifest_path or "",
            "finished_at_utc": bundle.run.finished_at_utc,
        },
        "row_counts": {
            "golden_records": len(bundle.golden_rows),
            "source_to_golden_crosswalk": len(bundle.crosswalk_rows),
        },
        "artifacts": artifacts,
    }
    missing_keys = set(DELIVERY_MANIFEST_KEYS) - set(manifest)
    if missing_keys:
        raise ValueError(f"Delivery manifest is missing required keys: {sorted(missing_keys)}")
    return manifest


def _existing_snapshot_is_compatible(
    *,
    snapshot_dir: Path,
    expected_run_id: str,
    contract_version: str,
) -> bool:
    manifest_path = snapshot_dir / "delivery_manifest.json"
    if not manifest_path.exists():
        return False
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return (
        manifest.get("contract_name") == DELIVERY_CONTRACT_NAME
        and manifest.get("contract_version") == contract_version
        and manifest.get("run_id") == expected_run_id
    )


def publish_delivery_snapshot(
    *,
    bundle: PersistedRunBundle,
    state_db_path: Path,
    output_root: Path,
    contract_version: str = DELIVERY_CONTRACT_VERSION,
    published_at_utc: str | None = None,
) -> PublishedDeliverySnapshot:
    if bundle.run.status != "completed":
        raise ValueError(f"Only completed runs can be published, received status={bundle.run.status!r}")

    published_timestamp = published_at_utc or _utc_now()
    snapshot_id = bundle.run.run_id
    contract_root = output_root / DELIVERY_CONTRACT_NAME / contract_version
    snapshots_root = contract_root / "snapshots"
    snapshot_dir = snapshots_root / snapshot_id
    manifest_path = snapshot_dir / "delivery_manifest.json"
    current_pointer_path = contract_root / "current.json"

    snapshots_root.mkdir(parents=True, exist_ok=True)

    if snapshot_dir.exists():
        if not _existing_snapshot_is_compatible(
            snapshot_dir=snapshot_dir,
            expected_run_id=bundle.run.run_id,
            contract_version=contract_version,
        ):
            raise FileExistsError(
                f"Delivery snapshot directory already exists with incompatible contents: {snapshot_dir}"
            )
    else:
        temporary_dir = snapshots_root / f".{snapshot_id}.{uuid4().hex}.tmp"
        temporary_dir.mkdir(parents=True, exist_ok=False)
        try:
            write_csv_dicts(
                temporary_dir / "golden_person_records.csv",
                bundle.golden_rows,
                fieldnames=DELIVERY_ARTIFACT_HEADERS[Path("golden_person_records.csv")],
            )
            write_csv_dicts(
                temporary_dir / "source_to_golden_crosswalk.csv",
                bundle.crosswalk_rows,
                fieldnames=DELIVERY_ARTIFACT_HEADERS[Path("source_to_golden_crosswalk.csv")],
            )
            _write_json(
                temporary_dir / "delivery_manifest.json",
                _build_delivery_manifest(
                    bundle=bundle,
                    state_db_path=state_db_path,
                    snapshot_id=snapshot_id,
                    contract_version=contract_version,
                    published_at_utc=published_timestamp,
                    snapshot_dir=temporary_dir,
                ),
            )
            temporary_dir.rename(snapshot_dir)
        finally:
            if temporary_dir.exists():
                for child in sorted(temporary_dir.rglob("*"), reverse=True):
                    if child.is_file():
                        child.unlink()
                    elif child.is_dir():
                        child.rmdir()
                temporary_dir.rmdir()

    current_pointer = {
        "contract_name": DELIVERY_CONTRACT_NAME,
        "contract_version": contract_version,
        "snapshot_id": snapshot_id,
        "run_id": bundle.run.run_id,
        "published_at_utc": published_timestamp,
        "relative_snapshot_path": str(snapshot_dir.relative_to(contract_root)).replace("\\", "/"),
        "relative_manifest_path": str(manifest_path.relative_to(contract_root)).replace("\\", "/"),
    }
    missing_pointer_keys = set(DELIVERY_CURRENT_POINTER_KEYS) - set(current_pointer)
    if missing_pointer_keys:
        raise ValueError(f"Delivery current pointer is missing required keys: {sorted(missing_pointer_keys)}")
    _write_atomic_json(current_pointer_path, current_pointer)

    return PublishedDeliverySnapshot(
        contract_root=contract_root,
        snapshot_dir=snapshot_dir,
        manifest_path=manifest_path,
        current_pointer_path=current_pointer_path,
        snapshot_id=snapshot_id,
    )
