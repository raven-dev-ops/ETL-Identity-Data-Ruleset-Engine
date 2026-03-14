"""Public-safety onboarding conformance checks."""

from __future__ import annotations

from pathlib import Path

from etl_identity_engine.ingest.manifest import resolve_batch_manifest
from etl_identity_engine.ingest.public_safety_contracts import (
    validate_public_safety_contract_bundle,
)


def _bundle_summary(bundle_dir: Path) -> dict[str, object]:
    validated = validate_public_safety_contract_bundle(bundle_dir)
    return validated.to_summary()


def _manifest_summary(manifest_path: Path) -> dict[str, object]:
    resolved = resolve_batch_manifest(manifest_path)
    return {
        "manifest_path": str(resolved.manifest_path),
        "batch_id": resolved.manifest.batch_id,
        "entity_type": resolved.manifest.entity_type,
        "landing_zone": {
            "kind": resolved.manifest.landing_zone.kind,
            "base_location": resolved.manifest.landing_zone.base_location,
        },
        "source_count": len(resolved.sources),
        "source_bundle_count": len(resolved.source_bundles),
        "sources": [
            {
                "source_id": source.spec.source_id,
                "source_reference": source.source_reference,
                "format": source.spec.format,
                "schema_version": source.spec.schema_version,
                "row_count": len(source.rows),
            }
            for source in resolved.sources
        ],
        "source_bundles": [
            {
                "bundle_id": bundle.spec.bundle_id,
                "source_class": bundle.spec.source_class,
                "bundle_reference": bundle.bundle_reference,
                "contract_name": bundle.contract_name,
                "contract_version": bundle.contract_version,
                "files": [
                    {
                        "logical_name": file.logical_name,
                        "relative_path": file.relative_path,
                        "format": file.format,
                        "row_count": file.row_count,
                    }
                    for file in bundle.files
                ],
            }
            for bundle in resolved.source_bundles
        ],
    }


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
    if bundle_dirs:
        summary["bundles"] = [_bundle_summary(bundle_dir.resolve()) for bundle_dir in bundle_dirs]
    if manifest_path is not None:
        summary["manifest"] = _manifest_summary(manifest_path.resolve())
    return summary
