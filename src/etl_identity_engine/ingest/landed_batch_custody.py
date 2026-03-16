"""Live landed-batch custody capture for packaged target packs."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import shutil

from etl_identity_engine.ingest.live_target_packs import check_live_target_pack, get_live_target_pack
from etl_identity_engine.ingest.manifest import resolve_batch_manifest


CUSTODY_MANIFEST_VERSION = "v1"
CUSTODY_MANIFEST_FILENAME = "custody_manifest.json"


class LandedBatchCustodyError(ValueError):
    """Raised when landed-batch custody inputs are incomplete or invalid."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_timestamp(value: str | None) -> str:
    if value is None or not value.strip():
        return _format_utc_timestamp(_utc_now())
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return _format_utc_timestamp(datetime.fromisoformat(normalized))
    except ValueError as exc:
        raise LandedBatchCustodyError(
            f"arrived_at_utc must be an ISO-8601 timestamp, received {value!r}"
        ) from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _classify_file(relative_path: Path) -> str:
    normalized = relative_path.as_posix()
    if normalized == "README.md":
        return "operator_readme"
    if normalized == "live_target_pack_summary.json":
        return "prepared_pack_summary"
    if normalized == "batch_manifest.yml":
        return "batch_manifest"
    if normalized.endswith("/contract_manifest.yml"):
        return "contract_manifest"
    if normalized.startswith("landing/"):
        return "landing_source"
    return "source_bundle_file"


def _immutable_capture_dir(target_root: Path, *, target_id: str, captured_at_utc: str) -> Path:
    timestamp_token = captured_at_utc.replace("-", "").replace(":", "").replace("T", "T").replace("Z", "Z")
    return target_root / f"{timestamp_token}-{target_id}"


def capture_live_target_custody(
    target_id: str,
    staged_root: Path,
    output_dir: Path,
    *,
    operator_id: str,
    transport_channel: str,
    tenant_id: str | None = None,
    arrived_at_utc: str | None = None,
) -> dict[str, object]:
    normalized_operator_id = operator_id.strip()
    if not normalized_operator_id:
        raise LandedBatchCustodyError("operator_id must be a non-empty string")
    normalized_transport_channel = transport_channel.strip()
    if not normalized_transport_channel:
        raise LandedBatchCustodyError("transport_channel must be a non-empty string")

    resolved_staged_root = staged_root.resolve()
    if not resolved_staged_root.exists():
        raise FileNotFoundError(f"Staged live target pack root not found: {resolved_staged_root}")
    if not resolved_staged_root.is_dir():
        raise NotADirectoryError(f"Staged live target pack root must be a directory: {resolved_staged_root}")

    pack = get_live_target_pack(target_id)
    validation = check_live_target_pack(target_id, resolved_staged_root)
    summary: dict[str, object] = {
        "custody_manifest_version": CUSTODY_MANIFEST_VERSION,
        "target_id": pack.target_id,
        "source_class": pack.source_class,
        "vendor_profile": pack.vendor_profile,
        "staged_root": str(resolved_staged_root),
        "operator_id": normalized_operator_id,
        "transport_channel": normalized_transport_channel,
        "arrived_at_utc": _parse_utc_timestamp(arrived_at_utc),
        "validation": validation,
    }
    if tenant_id is not None and tenant_id.strip():
        summary["tenant_id"] = tenant_id.strip()

    if validation["status"] != "passed":
        summary["status"] = "failed"
        summary["validation_error"] = "staged live target pack failed onboarding validation"
        return summary

    captured_at_utc = _format_utc_timestamp(_utc_now())
    immutable_root = _immutable_capture_dir(
        output_dir.resolve(),
        target_id=target_id,
        captured_at_utc=captured_at_utc,
    )
    if immutable_root.exists():
        raise LandedBatchCustodyError(f"Immutable landing directory already exists: {immutable_root}")
    immutable_root.mkdir(parents=True, exist_ok=False)

    tracked_files: list[dict[str, object]] = []
    for source_path in sorted(path for path in resolved_staged_root.rglob("*") if path.is_file()):
        relative_path = source_path.relative_to(resolved_staged_root)
        immutable_path = immutable_root / relative_path
        immutable_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, immutable_path)
        tracked_files.append(
            {
                "role": _classify_file(relative_path),
                "relative_path": relative_path.as_posix(),
                "original_filename": source_path.name,
                "original_path": str(source_path),
                "immutable_path": str(immutable_path),
                "size_bytes": immutable_path.stat().st_size,
                "sha256": _sha256(immutable_path),
            }
        )

    immutable_manifest_path = immutable_root / pack.manifest_name
    resolved_manifest = resolve_batch_manifest(immutable_manifest_path)
    replay_linkage = {
        "batch_id": resolved_manifest.manifest.batch_id,
        "manifest_path": str(immutable_manifest_path),
        "input_paths": list(resolved_manifest.input_paths),
        "source_bundle_ids": [bundle.spec.bundle_id for bundle in resolved_manifest.source_bundles],
        "target_id": pack.target_id,
    }
    if tenant_id is not None and tenant_id.strip():
        replay_linkage["tenant_id"] = tenant_id.strip()

    summary.update(
        {
            "status": "captured",
            "captured_at_utc": captured_at_utc,
            "immutable_root": str(immutable_root),
            "replay_linkage": replay_linkage,
            "tracked_files": tracked_files,
        }
    )

    custody_manifest_path = immutable_root / CUSTODY_MANIFEST_FILENAME
    custody_manifest_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    summary["custody_manifest_path"] = str(custody_manifest_path)
    custody_manifest_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary
