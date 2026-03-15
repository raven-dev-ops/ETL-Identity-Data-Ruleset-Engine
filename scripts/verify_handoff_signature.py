from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Sequence
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CUSTOMER_MANIFEST_NAME = "pilot_handoff_manifest.json"
DEFAULT_RELEASE_MANIFEST_NAME = "manifest.json"


def _ensure_repo_src_on_path() -> None:
    for candidate in (REPO_ROOT / "src", REPO_ROOT / "runtime" / "src"):
        if candidate.exists() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
            return


def _signature_sidecar_name(manifest_name: str) -> str:
    _ensure_repo_src_on_path()
    from etl_identity_engine.handoff_signing import signature_sidecar_name

    return signature_sidecar_name(manifest_name)


def _verify_signature(
    *,
    manifest_path: str,
    manifest_bytes: bytes,
    signature_payload: dict[str, Any],
    trusted_public_key_path: Path,
) -> dict[str, Any]:
    _ensure_repo_src_on_path()
    from etl_identity_engine.handoff_signing import verify_detached_signature

    return verify_detached_signature(
        manifest_path=manifest_path,
        manifest_bytes=manifest_bytes,
        signature_payload=signature_payload,
        trusted_public_key_path=trusted_public_key_path,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a detached handoff signature and, when available, replay manifest hash checks."
        )
    )
    parser.add_argument("--bundle", default=None, help="Packaged bundle zip to inspect.")
    parser.add_argument(
        "--bundle-root",
        default=None,
        help="Extracted bundle root to inspect.",
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help="Loose manifest path. Use with --signature for detached verification outside a bundle.",
    )
    parser.add_argument(
        "--signature",
        default=None,
        help="Loose detached signature path. Required with --manifest unless auto-discoverable.",
    )
    parser.add_argument(
        "--trusted-public-key",
        required=True,
        help="Trusted Ed25519 public key PEM used to verify the detached signature.",
    )
    parser.add_argument(
        "--manifest-name",
        default=None,
        help="Optional manifest name override when verifying a bundle or extracted bundle root.",
    )
    return parser.parse_args(argv)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _load_json_bytes(payload: bytes) -> dict[str, Any]:
    return json.loads(payload.decode("utf-8"))


def _resolve_source_mode(
    *,
    bundle: str | None,
    bundle_root: str | None,
    manifest: str | None,
    signature: str | None,
) -> tuple[str, Path, Path | None]:
    provided = [bool(bundle), bool(bundle_root), bool(manifest)]
    if sum(provided) != 1:
        raise ValueError("Provide exactly one of --bundle, --bundle-root, or --manifest")

    if bundle:
        return "bundle", Path(bundle).resolve(), None
    if bundle_root:
        return "bundle_root", Path(bundle_root).resolve(), None

    manifest_path = Path(manifest).resolve()
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    signature_path = Path(signature).resolve() if signature else manifest_path.with_name(
        _signature_sidecar_name(manifest_path.name)
    )
    return "manifest", manifest_path, signature_path


def _candidate_manifest_names(explicit_name: str | None = None) -> tuple[str, ...]:
    if explicit_name:
        return (explicit_name,)
    return (DEFAULT_CUSTOMER_MANIFEST_NAME, DEFAULT_RELEASE_MANIFEST_NAME)


def _verify_artifact_hash_entries(
    *,
    manifest_payload: dict[str, Any],
    artifact_lookup,
) -> tuple[list[dict[str, Any]], list[str]]:
    artifacts = manifest_payload.get("artifacts")
    if not isinstance(artifacts, list):
        return [], []

    checks: list[dict[str, Any]] = []
    errors: list[str] = []
    hash_entries = [entry for entry in artifacts if isinstance(entry, dict) and "sha256" in entry]
    if not hash_entries:
        return [], []

    for entry in hash_entries:
        relative_path = str(entry.get("path", "")).strip()
        expected_sha256 = str(entry.get("sha256", "")).strip()
        expected_size = entry.get("size_bytes")
        if not relative_path:
            errors.append("handoff manifest contains a hash entry without a path")
            continue
        try:
            payload = artifact_lookup(relative_path)
        except FileNotFoundError:
            checks.append({"check": f"artifact:{relative_path}", "status": "error", "detail": "missing"})
            errors.append(f"handoff manifest artifact is missing: {relative_path}")
            continue
        actual_sha256 = _sha256_bytes(payload)
        actual_size = len(payload)
        is_ok = actual_sha256 == expected_sha256 and actual_size == expected_size
        checks.append(
            {
                "check": f"artifact:{relative_path}",
                "status": "ok" if is_ok else "error",
                "detail": {
                    "expected_sha256": expected_sha256,
                    "actual_sha256": actual_sha256,
                    "expected_size": expected_size,
                    "actual_size": actual_size,
                },
            }
        )
        if not is_ok:
            errors.append(f"handoff manifest verification failed for {relative_path}")
    return checks, errors


def _load_bundle_manifest(
    *,
    bundle_path: Path,
    manifest_name: str | None,
) -> tuple[str, bytes, dict[str, Any], list[dict[str, Any]], list[str]]:
    with zipfile.ZipFile(bundle_path) as archive:
        members = set(archive.namelist())
        resolved_manifest_name = next(
            (candidate for candidate in _candidate_manifest_names(manifest_name) if candidate in members),
            None,
        )
        if resolved_manifest_name is None:
            raise FileNotFoundError("Unable to locate a supported manifest in the bundle")
        signature_name = _signature_sidecar_name(resolved_manifest_name)
        if signature_name not in members:
            raise FileNotFoundError(f"Detached signature not found in bundle: {signature_name}")
        manifest_bytes = archive.read(resolved_manifest_name)
        signature_payload = _load_json_bytes(archive.read(signature_name))

        def artifact_lookup(relative_path: str) -> bytes:
            if relative_path not in members:
                raise FileNotFoundError(relative_path)
            return archive.read(relative_path)

        artifact_checks, artifact_errors = _verify_artifact_hash_entries(
            manifest_payload=_load_json_bytes(manifest_bytes),
            artifact_lookup=artifact_lookup,
        )
    return resolved_manifest_name, manifest_bytes, signature_payload, artifact_checks, artifact_errors


def _load_bundle_root_manifest(
    *,
    bundle_root: Path,
    manifest_name: str | None,
) -> tuple[str, bytes, dict[str, Any], list[dict[str, Any]], list[str]]:
    resolved_manifest_name = next(
        (
            candidate
            for candidate in _candidate_manifest_names(manifest_name)
            if bundle_root.joinpath(*candidate.split("/")).exists()
        ),
        None,
    )
    if resolved_manifest_name is None:
        raise FileNotFoundError("Unable to locate a supported manifest in the bundle root")
    manifest_path = bundle_root / resolved_manifest_name
    signature_path = bundle_root / _signature_sidecar_name(resolved_manifest_name)
    if not signature_path.exists():
        raise FileNotFoundError(f"Detached signature not found in bundle root: {signature_path}")
    manifest_bytes = manifest_path.read_bytes()
    manifest_payload = _load_json_bytes(manifest_bytes)
    signature_payload = _load_json_bytes(signature_path.read_bytes())

    def artifact_lookup(relative_path: str) -> bytes:
        target = bundle_root.joinpath(*relative_path.split("/"))
        if not target.exists():
            raise FileNotFoundError(relative_path)
        return target.read_bytes()

    artifact_checks, artifact_errors = _verify_artifact_hash_entries(
        manifest_payload=manifest_payload,
        artifact_lookup=artifact_lookup,
    )
    return resolved_manifest_name, manifest_bytes, signature_payload, artifact_checks, artifact_errors


def verify_handoff_signature(
    *,
    bundle: str | None = None,
    bundle_root: str | None = None,
    manifest: str | None = None,
    signature: str | None = None,
    trusted_public_key: str,
    manifest_name: str | None = None,
) -> dict[str, Any]:
    source_mode, primary_path, secondary_path = _resolve_source_mode(
        bundle=bundle,
        bundle_root=bundle_root,
        manifest=manifest,
        signature=signature,
    )
    trusted_public_key_path = Path(trusted_public_key).resolve()
    if not trusted_public_key_path.exists():
        raise FileNotFoundError(f"Trusted public key not found: {trusted_public_key_path}")

    artifact_checks: list[dict[str, Any]] = []
    artifact_errors: list[str] = []
    if source_mode == "bundle":
        resolved_manifest_name, manifest_bytes, signature_payload, artifact_checks, artifact_errors = (
            _load_bundle_manifest(bundle_path=primary_path, manifest_name=manifest_name)
        )
    elif source_mode == "bundle_root":
        resolved_manifest_name, manifest_bytes, signature_payload, artifact_checks, artifact_errors = (
            _load_bundle_root_manifest(bundle_root=primary_path, manifest_name=manifest_name)
        )
    else:
        assert secondary_path is not None
        if not secondary_path.exists():
            raise FileNotFoundError(f"Detached signature not found: {secondary_path}")
        resolved_manifest_name = primary_path.name
        manifest_bytes = primary_path.read_bytes()
        signature_payload = _load_json_bytes(secondary_path.read_bytes())

    signature_result = _verify_signature(
        manifest_path=resolved_manifest_name,
        manifest_bytes=manifest_bytes,
        signature_payload=signature_payload,
        trusted_public_key_path=trusted_public_key_path,
    )
    errors = list(artifact_errors)
    return {
        "status": "error" if errors else "ok",
        "source_mode": source_mode,
        "manifest_path": resolved_manifest_name,
        "signature_path": _signature_sidecar_name(resolved_manifest_name),
        "trusted_public_key": str(trusted_public_key_path),
        "signature": signature_result,
        "checks": artifact_checks,
        "errors": errors,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = verify_handoff_signature(
        bundle=args.bundle,
        bundle_root=args.bundle_root,
        manifest=args.manifest,
        signature=args.signature,
        trusted_public_key=args.trusted_public_key,
        manifest_name=args.manifest_name,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
