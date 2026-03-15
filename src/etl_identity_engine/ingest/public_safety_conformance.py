"""Public-safety onboarding conformance checks."""

from __future__ import annotations

from pathlib import Path

from etl_identity_engine.ingest.manifest import (
    _load_batch_manifest,
    _resolve_local_bundle_overlay_path,
    _resolve_local_bundle_path,
    inspect_batch_manifest,
)
from etl_identity_engine.ingest.public_safety_contracts import (
    inspect_public_safety_contract_bundle,
)


def _bundle_summary(
    bundle_dir: Path,
    *,
    mapping_overlay_path: Path | None = None,
    vendor_profile: str | None = None,
) -> dict[str, object]:
    return inspect_public_safety_contract_bundle(
        bundle_dir,
        mapping_overlay_path=mapping_overlay_path,
        vendor_profile=vendor_profile,
    )


def _manifest_summary(manifest_path: Path) -> dict[str, object]:
    return inspect_batch_manifest(manifest_path)


def _bundle_inspection_hints_from_manifest(manifest_path: Path) -> dict[Path, dict[str, object]]:
    try:
        manifest = _load_batch_manifest(manifest_path)
    except (FileNotFoundError, ValueError):
        return {}
    if manifest.landing_zone.kind != "local_filesystem":
        return {}

    hints: dict[Path, dict[str, object]] = {}
    for bundle in manifest.source_bundles:
        bundle_path = _resolve_local_bundle_path(
            manifest_path,
            manifest.landing_zone,
            bundle,
        ).resolve()
        hints[bundle_path] = {
            "mapping_overlay_path": _resolve_local_bundle_overlay_path(bundle_path, bundle),
            "vendor_profile": bundle.vendor_profile,
        }
    return hints


def check_public_safety_onboarding(
    *,
    bundle_dirs: tuple[Path, ...] = (),
    manifest_path: Path | None = None,
) -> dict[str, object]:
    """Validate one or more public-safety bundles and an optional manifest."""
    if not bundle_dirs and manifest_path is None:
        raise ValueError("Provide at least one bundle directory or a manifest path")

    summary: dict[str, object] = {
        "status": "passed",
        "bundle_count": len(bundle_dirs),
    }
    bundle_hints = {}
    if manifest_path is not None:
        bundle_hints = _bundle_inspection_hints_from_manifest(manifest_path.resolve())
    if bundle_dirs:
        bundles = [
            _bundle_summary(
                bundle_dir.resolve(),
                mapping_overlay_path=bundle_hints.get(bundle_dir.resolve(), {}).get("mapping_overlay_path"),
                vendor_profile=bundle_hints.get(bundle_dir.resolve(), {}).get("vendor_profile"),
            )
            for bundle_dir in bundle_dirs
        ]
        summary["bundles"] = bundles
        if any(bundle["status"] != "passed" for bundle in bundles):
            summary["status"] = "failed"
    if manifest_path is not None:
        manifest_summary = _manifest_summary(manifest_path.resolve())
        summary["manifest"] = manifest_summary
        if manifest_summary["status"] != "passed":
            summary["status"] = "failed"
    return summary
