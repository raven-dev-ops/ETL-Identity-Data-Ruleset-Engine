"""Replay-bundle archival and verification for manifest-driven runs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
from urllib.parse import urlsplit
from uuid import uuid4

import yaml

from etl_identity_engine.ingest.manifest import ResolvedBatchManifest, ResolvedBatchSource


REPLAY_BUNDLE_VERSION = "1"
REPLAY_BUNDLE_RELATIVE_ROOT = Path("data") / "replay_bundles"
REPLAY_BUNDLE_STATUS_VERIFIED = "verified"
REPLAY_BUNDLE_STATUS_INCOMPLETE = "incomplete"
REPLAY_BUNDLE_RESTORE_MODE = "restore_original_manifest_and_landing_snapshot"


@dataclass(frozen=True)
class ReplayBundleArtifact:
    kind: str
    relative_path: str
    original_reference: str
    source_id: str | None
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class ReplayBundleVerificationResult:
    bundle_id: str
    run_id: str
    bundle_root: Path
    bundle_manifest_path: Path
    original_manifest_path: Path
    replay_manifest_path: Path
    landing_snapshot_root: Path
    created_at_utc: str
    artifact_count: int
    source_count: int
    total_bytes: int
    status: str
    recoverable: bool
    verified_at_utc: str
    errors: tuple[str, ...]
    artifacts: tuple[ReplayBundleArtifact, ...]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def replay_bundle_root_for_run(*, base_dir: Path, run_id: str) -> Path:
    return (base_dir / REPLAY_BUNDLE_RELATIVE_ROOT / run_id).resolve()


def replay_bundle_summary_from_result(result: ReplayBundleVerificationResult) -> dict[str, object]:
    return {
        "bundle_id": result.bundle_id,
        "bundle_version": REPLAY_BUNDLE_VERSION,
        "status": result.status,
        "recoverable": result.recoverable,
        "restore_mode": REPLAY_BUNDLE_RESTORE_MODE,
        "bundle_root": str(result.bundle_root),
        "bundle_manifest_path": str(result.bundle_manifest_path),
        "original_manifest_path": str(result.original_manifest_path),
        "replay_manifest_path": str(result.replay_manifest_path),
        "landing_snapshot_root": str(result.landing_snapshot_root),
        "created_at_utc": result.created_at_utc,
        "verified_at_utc": result.verified_at_utc,
        "artifact_count": result.artifact_count,
        "source_count": result.source_count,
        "total_bytes": result.total_bytes,
        "verification_errors": list(result.errors),
        "artifacts": [
            {
                "kind": artifact.kind,
                "relative_path": artifact.relative_path,
                "original_reference": artifact.original_reference,
                "source_id": artifact.source_id,
                "sha256": artifact.sha256,
                "size_bytes": artifact.size_bytes,
            }
            for artifact in result.artifacts
        ],
    }


def replay_bundle_manifest_path_from_summary(summary: Mapping[str, object]) -> Path | None:
    replay_bundle = summary.get("replay_bundle")
    if not isinstance(replay_bundle, Mapping):
        return None
    manifest_path = replay_bundle.get("bundle_manifest_path")
    if not isinstance(manifest_path, str) or not manifest_path.strip():
        return None
    return Path(manifest_path)


def archive_replay_bundle(
    *,
    run_id: str,
    base_dir: Path,
    resolved_manifest: ResolvedBatchManifest,
    created_at_utc: str | None = None,
) -> ReplayBundleVerificationResult:
    bundle_root = replay_bundle_root_for_run(base_dir=base_dir, run_id=run_id)
    if bundle_root.exists():
        raise FileExistsError(f"Replay bundle already exists for run_id={run_id}: {bundle_root}")

    temp_root = bundle_root.with_name(f"{bundle_root.name}.tmp-{uuid4().hex}")
    temp_root.parent.mkdir(parents=True, exist_ok=True)
    created = created_at_utc or utc_now()
    artifacts: list[ReplayBundleArtifact] = []
    replay_sources: list[dict[str, object]] = []

    original_manifest_relative = Path("manifest") / "original" / resolved_manifest.manifest_path.name
    replay_manifest_relative = Path("manifest") / "replay_manifest.yaml"
    original_manifest_path = temp_root / original_manifest_relative
    original_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    original_manifest_bytes = resolved_manifest.manifest_path.read_bytes()
    original_manifest_path.write_bytes(original_manifest_bytes)
    artifacts.append(
        _build_artifact(
            kind="manifest",
            relative_path=original_manifest_relative,
            original_reference=str(resolved_manifest.manifest_path),
            source_id=None,
            payload=original_manifest_bytes,
        )
    )

    for source in resolved_manifest.sources:
        bundle_relative_path, replay_source_path = _bundle_source_relative_paths(source)
        payload = _read_source_payload(resolved_manifest, source)
        target_path = temp_root / bundle_relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(payload)
        artifacts.append(
            _build_artifact(
                kind="source",
                relative_path=bundle_relative_path,
                original_reference=source.source_reference,
                source_id=source.spec.source_id,
                payload=payload,
            )
        )
        replay_sources.append(
            {
                "source_id": source.spec.source_id,
                "path": replay_source_path,
                "format": source.spec.format,
                "schema_version": source.spec.schema_version,
                "required_columns": list(source.spec.required_columns),
            }
        )

    replay_manifest_payload = yaml.safe_dump(
        {
            "manifest_version": resolved_manifest.manifest.manifest_version,
            "entity_type": resolved_manifest.manifest.entity_type,
            "batch_id": resolved_manifest.manifest.batch_id,
            "landing_zone": {
                "kind": "local_filesystem",
                "base_path": "./landing_snapshot",
            },
            "sources": replay_sources,
        },
        sort_keys=False,
    ).encode("utf-8")
    replay_manifest_path = temp_root / replay_manifest_relative
    replay_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    replay_manifest_path.write_bytes(replay_manifest_payload)
    artifacts.append(
        _build_artifact(
            kind="replay_manifest",
            relative_path=replay_manifest_relative,
            original_reference=str(resolved_manifest.manifest_path),
            source_id=None,
            payload=replay_manifest_payload,
        )
    )

    bundle_manifest_path = temp_root / "bundle_manifest.json"
    bundle_manifest_payload = {
        "bundle_version": REPLAY_BUNDLE_VERSION,
        "bundle_id": _bundle_id(run_id),
        "run_id": run_id,
        "created_at_utc": created,
        "restore_mode": REPLAY_BUNDLE_RESTORE_MODE,
        "original_manifest_path": _to_posix(original_manifest_relative),
        "replay_manifest_path": _to_posix(replay_manifest_relative),
        "landing_snapshot_root": "landing_snapshot",
        "artifacts": [
            {
                "kind": artifact.kind,
                "relative_path": artifact.relative_path,
                "original_reference": artifact.original_reference,
                "source_id": artifact.source_id,
                "sha256": artifact.sha256,
                "size_bytes": artifact.size_bytes,
            }
            for artifact in artifacts
        ],
    }
    bundle_manifest_path.write_text(
        json.dumps(bundle_manifest_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    shutil.move(str(temp_root), str(bundle_root))
    return verify_replay_bundle(bundle_root / "bundle_manifest.json")


def verify_replay_bundle(bundle_manifest_path: Path) -> ReplayBundleVerificationResult:
    resolved_manifest_path = bundle_manifest_path.resolve()
    if not resolved_manifest_path.exists():
        raise FileNotFoundError(f"Replay bundle manifest not found: {resolved_manifest_path}")

    payload = json.loads(resolved_manifest_path.read_text(encoding="utf-8"))
    bundle_root = resolved_manifest_path.parent
    bundle_id = str(payload.get("bundle_id", "") or "")
    run_id = str(payload.get("run_id", "") or "")
    created_at_utc = str(payload.get("created_at_utc", "") or "")
    original_manifest_path = bundle_root / str(payload.get("original_manifest_path", "") or "")
    replay_manifest_path = bundle_root / str(payload.get("replay_manifest_path", "") or "")
    landing_snapshot_root = bundle_root / str(payload.get("landing_snapshot_root", "landing_snapshot") or "landing_snapshot")

    artifacts: list[ReplayBundleArtifact] = []
    errors: list[str] = []
    total_bytes = 0
    for index, artifact_payload in enumerate(_payload_artifacts(payload)):
        try:
            artifact = _artifact_from_payload(artifact_payload)
        except ValueError as exc:
            errors.append(f"artifact[{index}]: {exc}")
            continue
        artifacts.append(artifact)
        total_bytes += artifact.size_bytes
        artifact_path = bundle_root / Path(artifact.relative_path)
        if not artifact_path.exists():
            errors.append(f"missing artifact: {artifact.relative_path}")
            continue
        if artifact_path.stat().st_size != artifact.size_bytes:
            errors.append(
                f"artifact size mismatch for {artifact.relative_path}: "
                f"expected {artifact.size_bytes}, found {artifact_path.stat().st_size}"
            )
        actual_sha256 = _hash_bytes(artifact_path.read_bytes())
        if actual_sha256 != artifact.sha256:
            errors.append(
                f"artifact hash mismatch for {artifact.relative_path}: "
                f"expected {artifact.sha256}, found {actual_sha256}"
            )

    source_count = sum(1 for artifact in artifacts if artifact.kind == "source")
    if source_count == 0:
        errors.append("bundle is missing archived source payloads")
    if not original_manifest_path.exists():
        errors.append(f"missing original manifest copy: {_to_posix(original_manifest_path.relative_to(bundle_root))}")
    if not replay_manifest_path.exists():
        errors.append(f"missing replay manifest copy: {_to_posix(replay_manifest_path.relative_to(bundle_root))}")
    else:
        replay_manifest = yaml.safe_load(replay_manifest_path.read_text(encoding="utf-8"))
        if not isinstance(replay_manifest, Mapping):
            errors.append("replay manifest is not a mapping")
        else:
            landing_zone = replay_manifest.get("landing_zone")
            if not isinstance(landing_zone, Mapping) or landing_zone.get("kind") != "local_filesystem":
                errors.append("replay manifest landing_zone must be local_filesystem")

    verified_at_utc = utc_now()
    recoverable = not errors
    status = REPLAY_BUNDLE_STATUS_VERIFIED if recoverable else REPLAY_BUNDLE_STATUS_INCOMPLETE
    return ReplayBundleVerificationResult(
        bundle_id=bundle_id,
        run_id=run_id,
        bundle_root=bundle_root,
        bundle_manifest_path=resolved_manifest_path,
        original_manifest_path=original_manifest_path,
        replay_manifest_path=replay_manifest_path,
        landing_snapshot_root=landing_snapshot_root,
        created_at_utc=created_at_utc,
        artifact_count=len(artifacts),
        source_count=source_count,
        total_bytes=total_bytes,
        status=status,
        recoverable=recoverable,
        verified_at_utc=verified_at_utc,
        errors=tuple(errors),
        artifacts=tuple(artifacts),
    )


def _build_artifact(
    *,
    kind: str,
    relative_path: Path,
    original_reference: str,
    source_id: str | None,
    payload: bytes,
) -> ReplayBundleArtifact:
    return ReplayBundleArtifact(
        kind=kind,
        relative_path=_to_posix(relative_path),
        original_reference=original_reference,
        source_id=source_id,
        sha256=_hash_bytes(payload),
        size_bytes=len(payload),
    )


def _payload_artifacts(payload: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("bundle manifest is missing an artifacts list")
    return tuple(
        item
        for item in artifacts
        if isinstance(item, Mapping)
    )


def _artifact_from_payload(payload: Mapping[str, object]) -> ReplayBundleArtifact:
    relative_path = str(payload.get("relative_path", "") or "")
    if not relative_path:
        raise ValueError("relative_path must be a non-empty string")
    pure_path = PurePosixPath(relative_path)
    if pure_path.is_absolute() or ".." in pure_path.parts:
        raise ValueError(f"relative_path must stay within the bundle root: {relative_path}")

    sha256 = str(payload.get("sha256", "") or "")
    if not sha256:
        raise ValueError(f"sha256 is missing for artifact {relative_path}")

    size_bytes = payload.get("size_bytes")
    if not isinstance(size_bytes, int):
        raise ValueError(f"size_bytes is missing for artifact {relative_path}")

    source_id = payload.get("source_id")
    normalized_source_id = None if source_id in (None, "") else str(source_id)
    return ReplayBundleArtifact(
        kind=str(payload.get("kind", "") or ""),
        relative_path=relative_path,
        original_reference=str(payload.get("original_reference", "") or ""),
        source_id=normalized_source_id,
        sha256=sha256,
        size_bytes=size_bytes,
    )


def _bundle_id(run_id: str) -> str:
    return f"replay-bundle-{run_id}"


def _bundle_source_relative_paths(source: ResolvedBatchSource) -> tuple[Path, str]:
    declared_path = source.spec.path.replace("\\", "/").strip()
    if declared_path and not _looks_absolute_or_remote_path(declared_path):
        pure_path = PurePosixPath(declared_path)
        if pure_path.parts and ".." not in pure_path.parts:
            bundle_relative = Path("landing_snapshot").joinpath(*pure_path.parts)
            replay_relative = _to_posix(Path(*pure_path.parts))
            return bundle_relative, replay_relative

    filename = _bundle_source_filename(source)
    bundle_relative = Path("landing_snapshot") / source.spec.source_id / filename
    replay_relative = _to_posix(Path(source.spec.source_id) / filename)
    return bundle_relative, replay_relative


def _bundle_source_filename(source: ResolvedBatchSource) -> str:
    parsed = urlsplit(source.source_reference)
    candidate_name = Path(parsed.path).name if parsed.scheme else Path(source.source_reference).name
    if not candidate_name:
        candidate_name = f"{source.spec.source_id}.{source.spec.format}"
    if Path(candidate_name).suffix.lower().lstrip(".") != source.spec.format:
        candidate_name = f"{candidate_name}.{source.spec.format}"
    return candidate_name


def _looks_absolute_or_remote_path(value: str) -> bool:
    if "://" in value:
        return True
    pure_path = PurePosixPath(value)
    if pure_path.is_absolute():
        return True
    return len(value) >= 3 and value[1] == ":" and value[2] in ("\\", "/")


def _read_source_payload(resolved_manifest: ResolvedBatchManifest, source: ResolvedBatchSource) -> bytes:
    if resolved_manifest.manifest.landing_zone.kind == "local_filesystem":
        return Path(source.source_reference).read_bytes()

    try:
        import fsspec
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Replay-bundle archiving for object-storage manifests requires `fsspec`."
        ) from exc

    with fsspec.open(
        source.source_reference,
        "rb",
        **resolved_manifest.manifest.landing_zone.storage_options,
    ) as handle:
        return handle.read()


def _hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _to_posix(path: Path) -> str:
    return path.as_posix()
