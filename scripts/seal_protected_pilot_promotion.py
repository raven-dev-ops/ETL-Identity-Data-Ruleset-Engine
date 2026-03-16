from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = Path("dist") / "protected-pilot-promotions"
PROMOTION_MANIFEST_VERSION = "v1"
PROMOTION_MANIFEST_NAME = "protected_pilot_promotion_manifest.json"
PROMOTION_SUMMARY_NAME = "protected_pilot_promotion_summary.md"
RUNTIME_ENV_FINGERPRINT_NAME = "runtime_env_fingerprint.json"
RUNTIME_CONFIG_SNAPSHOT_NAME = "reference/runtime_environments.yml"
PILOT_MANIFEST_COPY_NAME = "inputs/pilot_manifest.json"
PILOT_HANDOFF_MANIFEST_COPY_NAME = "inputs/pilot_handoff_manifest.json"
PILOT_HANDOFF_SIGNATURE_COPY_NAME = "inputs/pilot_handoff_manifest.sig.json"
CUSTODY_MANIFEST_COPY_NAME = "inputs/custody_manifest.json"
ACCEPTANCE_SUMMARY_COPY_NAME = "inputs/acceptance_package_summary.json"
HA_REHEARSAL_SUMMARY_COPY_NAME = "inputs/postgresql_ha_rehearsal.json"
EVIDENCE_MANIFEST_COPY_NAME = "inputs/cjis_evidence_manifest.json"
REQUIRED_HA_REHEARSAL_STEPS = (
    "schema_upgrade_against_writer_endpoint",
    "service_reconnected_after_writer_failover",
    "backup_restored_to_clean_postgresql_target",
    "replay_recovered_run_from_restored_postgresql_state",
)


def _ensure_repo_paths_on_path() -> None:
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    src_dir = REPO_ROOT / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


_ensure_repo_paths_on_path()


class ProtectedPilotPromotionError(ValueError):
    """Raised when a protected-pilot promotion seal input is invalid."""


def _load_customer_pilot_readiness_module():
    import check_customer_pilot_readiness as readiness

    return readiness


def _load_cjis_evidence_module():
    import package_cjis_evidence_pack as evidence_pack

    return evidence_pack


def _resolve_output_dir(output_dir: str) -> Path:
    from package_release_sample import resolve_output_dir

    return resolve_output_dir(output_dir, repo_root=REPO_ROOT)


def _resolve_generated_at_utc(*, explicit_value: str | None = None) -> str:
    from package_release_sample import resolve_generated_at_utc

    return resolve_generated_at_utc(repo_root=REPO_ROOT, explicit_value=explicit_value)


def _resolve_source_commit() -> str:
    from package_release_sample import resolve_source_commit

    return resolve_source_commit(REPO_ROOT)


def _current_state_store_revision(state_db: str | Path) -> str | None:
    from etl_identity_engine.storage.migration_runner import current_state_store_revision

    return current_state_store_revision(state_db)


def _head_revision() -> str:
    from etl_identity_engine.storage.migration_runner import head_revision

    return head_revision()


def _resolve_state_store_target(state_db: str | Path):
    from etl_identity_engine.storage.state_store_target import resolve_state_store_target

    return resolve_state_store_target(state_db)


def _load_runtime_environment(*args, **kwargs):
    from etl_identity_engine.runtime_config import load_runtime_environment

    return load_runtime_environment(*args, **kwargs)


def _signature_sidecar_name(manifest_name: str) -> str:
    from etl_identity_engine.handoff_signing import signature_sidecar_name

    return signature_sidecar_name(manifest_name)


def _write_detached_signature(
    *,
    destination: Path,
    manifest_path: str,
    manifest_bytes: bytes,
    private_key_path: Path,
    signer_identity: str | None,
    key_id: str | None,
) -> dict[str, str]:
    from etl_identity_engine.handoff_signing import write_detached_signature

    return write_detached_signature(
        destination=destination,
        manifest_path=manifest_path,
        manifest_bytes=manifest_bytes,
        private_key_path=private_key_path,
        signer_identity=signer_identity,
        key_id=key_id,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seal the immutable artifact, config, evidence, and rollback inputs for a protected pilot promotion."
        )
    )
    bundle_group = parser.add_mutually_exclusive_group(required=True)
    bundle_group.add_argument("--bundle", default=None, help="Packaged customer pilot bundle zip.")
    bundle_group.add_argument(
        "--bundle-root",
        default=None,
        help="Extracted customer pilot bundle root.",
    )
    parser.add_argument(
        "--trusted-public-key",
        default=None,
        help="Trusted Ed25519 public key PEM for detached pilot handoff verification when present.",
    )
    parser.add_argument(
        "--runtime-config",
        default=str(REPO_ROOT / "config" / "runtime_environments.yml"),
        help="Runtime environment catalog for the protected pilot environment.",
    )
    parser.add_argument(
        "--environment",
        default="cjis",
        help="Runtime environment name for the protected pilot deployment.",
    )
    parser.add_argument(
        "--env-file",
        required=True,
        help="KEY=VALUE runtime environment snapshot for the protected pilot deployment.",
    )
    parser.add_argument(
        "--state-db",
        default=None,
        help="Optional state-store override. Defaults to the selected runtime environment state_db.",
    )
    parser.add_argument("--custody-manifest", required=True, help="Captured landed-batch custody manifest.")
    parser.add_argument(
        "--acceptance-summary",
        required=True,
        help="Masked live-target acceptance package summary JSON.",
    )
    parser.add_argument(
        "--evidence-pack",
        required=True,
        help="CJIS evidence pack zip produced for the protected pilot baseline.",
    )
    parser.add_argument(
        "--ha-rehearsal-summary",
        required=True,
        help="JSON summary captured from scripts/postgresql_ha_rehearsal.py.",
    )
    parser.add_argument(
        "--rollback-bundle",
        required=True,
        help="Immutable rollback bundle recorded before cutover, typically backup-state-bundle output.",
    )
    parser.add_argument(
        "--promotion-label",
        default=None,
        help="Optional label to embed in the promotion output directory and manifest.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the sealed promotion manifest and summary will be written.",
    )
    parser.add_argument(
        "--signing-key",
        default=None,
        help="Optional Ed25519 private key PEM used to sign the promotion manifest.",
    )
    parser.add_argument(
        "--signer-identity",
        default=None,
        help="Optional signer identity for the promotion-manifest detached signature.",
    )
    parser.add_argument(
        "--key-id",
        default=None,
        help="Optional key identifier for the promotion-manifest detached signature.",
    )
    return parser.parse_args(argv)


def _read_json_bytes(payload: bytes, *, context: str) -> dict[str, object]:
    try:
        loaded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtectedPilotPromotionError(f"{context} must be valid UTF-8 JSON") from exc
    if not isinstance(loaded, dict):
        raise ProtectedPilotPromotionError(f"{context} must decode to a JSON object")
    return loaded


def _read_json_file(path: Path, *, context: str) -> dict[str, object]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProtectedPilotPromotionError(f"{context} is not readable JSON: {path}") from exc
    if not isinstance(loaded, dict):
        raise ProtectedPilotPromotionError(f"{context} must decode to a JSON object: {path}")
    return loaded


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_timestamp(path: Path) -> str:
    return (
        datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _file_reference(path: Path) -> dict[str, object]:
    resolved = path.resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256_path(resolved),
        "size_bytes": resolved.stat().st_size,
        "modified_at_utc": _file_timestamp(resolved),
    }


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-")
    if not normalized:
        raise ProtectedPilotPromotionError("promotion label must contain at least one letter or number")
    return normalized


def _promotion_root(output_dir: Path, *, generated_at_utc: str, label: str) -> Path:
    timestamp_token = generated_at_utc.replace("-", "").replace(":", "")
    return output_dir / f"{timestamp_token}-{label}-promotion"


def _copy_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def _copy_json_input(source: Path, destination: Path) -> dict[str, object]:
    _copy_file(source, destination)
    return _read_json_file(destination, context=f"copied input {destination.name}")


def _parse_env_file(env_file: Path) -> dict[str, str]:
    evidence_pack = _load_cjis_evidence_module()
    return evidence_pack.load_env_file(env_file)


def _build_runtime_environment_summary(
    *,
    environment_name: str,
    runtime_config_path: Path,
    env_file: Path,
) -> tuple[dict[str, object], dict[str, str], str | Path]:
    evidence_pack = _load_cjis_evidence_module()
    effective_environ = evidence_pack.build_effective_environ(env_file=env_file, environ={})
    runtime_summary = evidence_pack.build_runtime_environment_summary(
        environment_name=environment_name,
        runtime_config_path=runtime_config_path,
        effective_environ=effective_environ,
    )
    runtime_environment = _load_runtime_environment(
        environment_name,
        runtime_config_path,
        environ=effective_environ,
    )
    resolved_state_db = runtime_environment.state_db
    if resolved_state_db is None:
        raise ProtectedPilotPromotionError(
            f"runtime environment {environment_name!r} did not resolve a state_db for promotion sealing"
        )
    return runtime_summary, effective_environ, resolved_state_db


def _runtime_env_fingerprint(path: Path) -> dict[str, object]:
    values = _parse_env_file(path)
    return {
        "path": str(path.resolve()),
        "sha256": _sha256_path(path),
        "size_bytes": path.stat().st_size,
        "declared_variable_count": len(values),
        "declared_variables": sorted(values),
        "file_reference_variables": sorted(key for key in values if key.endswith("_FILE")),
    }


def _state_store_summary(state_db: str | Path) -> dict[str, object]:
    target = _resolve_state_store_target(state_db)
    current_revision = _current_state_store_revision(state_db)
    head_revision = _head_revision()
    return {
        "display_name": target.display_name,
        "backend": target.backend,
        "current_revision": current_revision or "uninitialized",
        "head_revision": head_revision,
    }


def _required_check(check: str, condition: bool, detail: object) -> dict[str, object]:
    return {
        "check": check,
        "status": "ok" if condition else "error",
        "detail": detail,
    }


def _load_customer_pilot_bundle(
    *,
    bundle_path: Path | None,
    bundle_root: Path | None,
    trusted_public_key: Path | None,
) -> dict[str, object]:
    readiness = _load_customer_pilot_readiness_module()
    trusted_public_key_path = readiness._resolve_trusted_public_key_path(
        None if trusted_public_key is None else str(trusted_public_key)
    )
    pilot_manifest: dict[str, object]
    pilot_manifest_bytes: bytes
    handoff_manifest: dict[str, object]
    handoff_manifest_bytes: bytes
    signature_payload: dict[str, object] | None = None
    signature_bytes: bytes | None = None
    bundle_record: dict[str, object]

    if bundle_path is not None:
        resolved_bundle_path = bundle_path.resolve()
        with zipfile.ZipFile(resolved_bundle_path) as archive:
            pilot_manifest_bytes = archive.read(readiness.PILOT_MANIFEST_NAME)
            pilot_manifest = _read_json_bytes(
                pilot_manifest_bytes,
                context=readiness.PILOT_MANIFEST_NAME,
            )
            handoff_manifest, _checks, errors = readiness._inspect_bundle_zip(
                resolved_bundle_path,
                trusted_public_key_path=trusted_public_key_path,
            )
            signature_name = readiness._signature_sidecar_name(readiness.HANDOFF_MANIFEST_NAME)
            if signature_name in archive.namelist():
                signature_bytes = archive.read(signature_name)
                signature_payload = _read_json_bytes(signature_bytes, context=signature_name)
            handoff_manifest_bytes = archive.read(readiness.HANDOFF_MANIFEST_NAME)
        bundle_record = {
            "artifact_kind": "bundle_zip",
            **_file_reference(resolved_bundle_path),
        }
    else:
        assert bundle_root is not None
        resolved_bundle_root = bundle_root.resolve()
        pilot_manifest_path = resolved_bundle_root / readiness.PILOT_MANIFEST_NAME
        handoff_manifest_path = resolved_bundle_root / readiness.HANDOFF_MANIFEST_NAME
        pilot_manifest = _read_json_file(pilot_manifest_path, context=pilot_manifest_path.name)
        handoff_manifest, _checks, errors = readiness._inspect_bundle_root(
            resolved_bundle_root,
            trusted_public_key_path=trusted_public_key_path,
        )
        handoff_manifest_bytes = handoff_manifest_path.read_bytes()
        pilot_manifest_bytes = pilot_manifest_path.read_bytes()
        signature_name = readiness._signature_sidecar_name(readiness.HANDOFF_MANIFEST_NAME)
        signature_path = resolved_bundle_root / signature_name
        if signature_path.exists():
            signature_bytes = signature_path.read_bytes()
            signature_payload = _read_json_bytes(signature_bytes, context=signature_path.name)
        bundle_record = {
            "artifact_kind": "bundle_root",
            "path": str(resolved_bundle_root),
            "handoff_manifest_sha256": _sha256_bytes(handoff_manifest_bytes),
            "pilot_manifest_sha256": _sha256_bytes(pilot_manifest_bytes),
        }

    if errors:
        detail = "; ".join(errors)
        raise ProtectedPilotPromotionError(f"customer pilot bundle handoff verification failed: {detail}")

    return {
        "pilot_manifest": pilot_manifest,
        "pilot_manifest_bytes": pilot_manifest_bytes,
        "handoff_manifest": handoff_manifest,
        "handoff_manifest_bytes": handoff_manifest_bytes,
        "signature_payload": signature_payload,
        "signature_bytes": signature_bytes,
        "bundle_record": bundle_record,
        "trusted_public_key": None if trusted_public_key_path is None else str(trusted_public_key_path),
    }


def _load_custody_manifest(path: Path) -> dict[str, object]:
    payload = _read_json_file(path, context="custody manifest")
    if payload.get("status") != "captured":
        raise ProtectedPilotPromotionError(
            f"custody manifest must have status 'captured': {path.resolve()}"
        )
    return payload


def _load_acceptance_summary(path: Path) -> dict[str, object]:
    payload = _read_json_file(path, context="acceptance summary")
    if payload.get("status") != "packaged":
        raise ProtectedPilotPromotionError(
            f"acceptance package summary must have status 'packaged': {path.resolve()}"
        )
    masked_validation = payload.get("masked_validation", {})
    if not isinstance(masked_validation, dict) or masked_validation.get("status") != "passed":
        raise ProtectedPilotPromotionError(
            f"acceptance package summary must include a passed masked_validation result: {path.resolve()}"
        )
    return payload


def _load_ha_rehearsal_summary(path: Path) -> dict[str, object]:
    payload = _read_json_file(path, context="HA rehearsal summary")
    if payload.get("status") != "ok":
        raise ProtectedPilotPromotionError(
            f"HA rehearsal summary must have status 'ok': {path.resolve()}"
        )
    steps = payload.get("validated_steps", [])
    if not isinstance(steps, list):
        raise ProtectedPilotPromotionError("HA rehearsal summary validated_steps must be a list")
    missing_steps = [step for step in REQUIRED_HA_REHEARSAL_STEPS if step not in steps]
    if missing_steps:
        raise ProtectedPilotPromotionError(
            "HA rehearsal summary is missing required validated steps: "
            + ", ".join(missing_steps)
        )
    return payload


def _load_evidence_manifest(path: Path) -> dict[str, object]:
    evidence_pack = _load_cjis_evidence_module()
    with zipfile.ZipFile(path) as archive:
        manifest_bytes = archive.read(evidence_pack.EVIDENCE_MANIFEST_NAME)
    payload = _read_json_bytes(
        manifest_bytes,
        context=evidence_pack.EVIDENCE_MANIFEST_NAME,
    )
    if payload.get("bundle_type") != "cjis_evidence_pack":
        raise ProtectedPilotPromotionError(
            f"evidence pack does not contain a CJIS evidence manifest: {path.resolve()}"
        )
    if payload.get("preflight_status") != "ok":
        raise ProtectedPilotPromotionError(
            f"evidence pack preflight_status must be 'ok': {path.resolve()}"
        )
    return payload


def _input_reference(
    path: Path,
    *,
    copied_relative_path: str | None = None,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = _file_reference(path)
    if copied_relative_path is not None:
        payload["copied_relative_path"] = copied_relative_path
    if extra:
        payload.update(extra)
    return payload


def _build_revalidation_commands(
    *,
    environment_name: str,
    runtime_config_path: Path,
    env_file: Path,
    bundle_path: Path | None,
    bundle_root: Path | None,
) -> tuple[str, ...]:
    bundle_argument = (
        f"--bundle \"{bundle_path.resolve()}\""
        if bundle_path is not None
        else f"--bundle-root \"{bundle_root.resolve()}\""
    )
    return (
        f"python scripts/check_customer_pilot_readiness.py {bundle_argument} --output dist/protected-pilot-revalidation/pilot_readiness.json",
        f"python scripts/cjis_preflight_check.py --environment {environment_name} --runtime-config \"{runtime_config_path.resolve()}\" --output dist/protected-pilot-revalidation/cjis_preflight.json",
        f"python scripts/package_cjis_evidence_pack.py --environment {environment_name} --runtime-config \"{runtime_config_path.resolve()}\" --env-file \"{env_file.resolve()}\" --output-dir dist/protected-pilot-revalidation/cjis-evidence",
    )


def _build_rollback_steps(*, rollback_bundle: Path) -> tuple[str, ...]:
    return (
        "Stop the protected pilot service and batch entrypoints before restoring state.",
        "Restore the recorded rollback bundle into the protected pilot state store with restore-state-bundle.",
        f"Recorded rollback artifact: {rollback_bundle.resolve()}",
        "Restore the replay-bundle attachments required by the protected pilot runtime.",
        "Rerun the documented readiness, preflight, and evidence-pack revalidation commands before reopening operator or consumer access.",
    )


def _build_summary_markdown(manifest: dict[str, object]) -> str:
    lines = [
        "# Protected Pilot Promotion Seal",
        "",
        f"- promotion_label: `{manifest['promotion_label']}`",
        f"- generated_at_utc: `{manifest['generated_at_utc']}`",
        f"- pilot_name: `{manifest['pilot_name']}`",
        f"- target_id: `{manifest['target_id']}`",
        f"- environment: `{manifest['environment']}`",
        "",
        "## Immutable Inputs",
        "",
    ]
    for name, details in manifest["inputs"].items():
        lines.append(
            f"- {name}: `{details['path']}`"
            if "path" in details
            else f"- {name}: `{details['artifact_kind']}`"
        )
        if "sha256" in details:
            lines.append(f"  sha256: `{details['sha256']}`")
    lines.extend(
        [
            "",
            "## Rollback",
            "",
        ]
    )
    for step in manifest["rollback"]["steps"]:
        lines.append(f"- {step}")
    lines.extend(
        [
            "",
            "## Revalidation",
            "",
        ]
    )
    for command in manifest["revalidation"]["commands"]:
        lines.append(f"- `{command}`")
    return "\n".join(lines).strip() + "\n"


def seal_protected_pilot_promotion(
    *,
    output_dir: Path,
    bundle: Path | None,
    bundle_root: Path | None,
    trusted_public_key: Path | None,
    runtime_config_path: Path,
    environment_name: str,
    env_file: Path,
    state_db: str | Path | None,
    custody_manifest_path: Path,
    acceptance_summary_path: Path,
    evidence_pack_path: Path,
    ha_rehearsal_summary_path: Path,
    rollback_bundle_path: Path,
    promotion_label: str | None = None,
    generated_at_utc: str | None = None,
    source_commit: str | None = None,
    signing_key: Path | None = None,
    signer_identity: str | None = None,
    key_id: str | None = None,
) -> dict[str, object]:
    if bundle is None and bundle_root is None:
        raise ProtectedPilotPromotionError("Provide either bundle or bundle_root for promotion sealing")

    resolved_output_dir = output_dir.resolve()
    resolved_runtime_config_path = runtime_config_path.resolve()
    resolved_env_file = env_file.resolve()
    if not resolved_runtime_config_path.exists():
        raise FileNotFoundError(f"Runtime config not found: {resolved_runtime_config_path}")
    if not resolved_env_file.exists():
        raise FileNotFoundError(f"Runtime env snapshot not found: {resolved_env_file}")

    bundle_summary = _load_customer_pilot_bundle(
        bundle_path=None if bundle is None else bundle.resolve(),
        bundle_root=None if bundle_root is None else bundle_root.resolve(),
        trusted_public_key=None if trusted_public_key is None else trusted_public_key.resolve(),
    )
    custody_manifest = _load_custody_manifest(custody_manifest_path.resolve())
    acceptance_summary = _load_acceptance_summary(acceptance_summary_path.resolve())
    _load_ha_rehearsal_summary(ha_rehearsal_summary_path.resolve())
    evidence_manifest = _load_evidence_manifest(evidence_pack_path.resolve())
    if not rollback_bundle_path.resolve().exists():
        raise FileNotFoundError(f"Rollback bundle not found: {rollback_bundle_path.resolve()}")

    target_id = str(custody_manifest.get("target_id", "") or "").strip()
    if not target_id:
        raise ProtectedPilotPromotionError("custody manifest is missing target_id")
    if acceptance_summary.get("target_id") != target_id:
        raise ProtectedPilotPromotionError(
            "acceptance package target_id does not match the custody manifest target_id"
        )
    if acceptance_summary.get("source_custody_manifest_present") is not True:
        raise ProtectedPilotPromotionError(
            "acceptance package summary must confirm that it was generated from a captured custody root"
        )
    if evidence_manifest.get("environment") != environment_name:
        raise ProtectedPilotPromotionError(
            "evidence pack environment does not match the requested protected pilot environment"
        )

    runtime_summary, _effective_environ, resolved_runtime_state_db = _build_runtime_environment_summary(
        environment_name=environment_name,
        runtime_config_path=resolved_runtime_config_path,
        env_file=resolved_env_file,
    )
    resolved_state_db = state_db or resolved_runtime_state_db
    state_store_summary = _state_store_summary(resolved_state_db)
    state_db_text = str(resolved_state_db)
    pilot_manifest = bundle_summary["pilot_manifest"]
    pilot_name = str(pilot_manifest.get("pilot_name", "") or "").strip()
    if not pilot_name:
        raise ProtectedPilotPromotionError("customer pilot bundle is missing pilot_name")
    bundle_version = str(pilot_manifest.get("version", "") or "").strip()
    if not bundle_version:
        raise ProtectedPilotPromotionError("customer pilot bundle is missing version")

    resolved_generated_at_utc = _resolve_generated_at_utc(explicit_value=generated_at_utc)
    label = promotion_label or f"{pilot_name}-{target_id}-{environment_name}"
    resolved_label = _slugify(label)
    promotion_root = _promotion_root(
        resolved_output_dir,
        generated_at_utc=resolved_generated_at_utc,
        label=resolved_label,
    )
    if promotion_root.exists():
        raise ProtectedPilotPromotionError(f"Promotion output already exists: {promotion_root}")
    promotion_root.mkdir(parents=True, exist_ok=False)

    _copy_file(resolved_runtime_config_path, promotion_root / RUNTIME_CONFIG_SNAPSHOT_NAME)
    env_fingerprint = _runtime_env_fingerprint(resolved_env_file)
    _write_json(promotion_root / RUNTIME_ENV_FINGERPRINT_NAME, env_fingerprint)
    _copy_bytes(promotion_root / PILOT_MANIFEST_COPY_NAME, bundle_summary["pilot_manifest_bytes"])
    _copy_bytes(
        promotion_root / PILOT_HANDOFF_MANIFEST_COPY_NAME,
        bundle_summary["handoff_manifest_bytes"],
    )
    if bundle_summary["signature_bytes"] is not None:
        _copy_bytes(
            promotion_root / PILOT_HANDOFF_SIGNATURE_COPY_NAME,
            bundle_summary["signature_bytes"],
        )

    copied_custody_manifest = _copy_json_input(
        custody_manifest_path.resolve(),
        promotion_root / CUSTODY_MANIFEST_COPY_NAME,
    )
    copied_acceptance_summary = _copy_json_input(
        acceptance_summary_path.resolve(),
        promotion_root / ACCEPTANCE_SUMMARY_COPY_NAME,
    )
    copied_ha_rehearsal_summary = _copy_json_input(
        ha_rehearsal_summary_path.resolve(),
        promotion_root / HA_REHEARSAL_SUMMARY_COPY_NAME,
    )
    _write_json(promotion_root / EVIDENCE_MANIFEST_COPY_NAME, evidence_manifest)

    tenant_id = (
        str(copied_custody_manifest.get("tenant_id", "") or "").strip()
        or str(copied_custody_manifest.get("replay_linkage", {}).get("tenant_id", "") or "").strip()
        or None
    )
    checks = [
        _required_check("pilot_bundle_handoff_verification", True, "passed"),
        _required_check(
            "custody_target_alignment",
            copied_acceptance_summary.get("target_id") == copied_custody_manifest.get("target_id"),
            {
                "custody_target_id": copied_custody_manifest.get("target_id"),
                "acceptance_target_id": copied_acceptance_summary.get("target_id"),
            },
        ),
        _required_check(
            "acceptance_generated_from_custody",
            copied_acceptance_summary.get("source_custody_manifest_present") is True,
            copied_acceptance_summary.get("source_custody_manifest_present"),
        ),
        _required_check(
            "evidence_environment",
            evidence_manifest.get("environment") == environment_name,
            evidence_manifest.get("environment"),
        ),
        _required_check(
            "evidence_preflight",
            evidence_manifest.get("preflight_status") == "ok",
            evidence_manifest.get("preflight_status"),
        ),
        _required_check(
            "ha_rehearsal_status",
            copied_ha_rehearsal_summary.get("status") == "ok",
            copied_ha_rehearsal_summary.get("status"),
        ),
        _required_check(
            "state_store_backend",
            state_store_summary["backend"] == "postgresql",
            state_store_summary["display_name"],
        ),
        _required_check(
            "state_store_writer_endpoint",
            "target_session_attrs=read-write" in state_db_text,
            state_store_summary["display_name"],
        ),
        _required_check(
            "state_store_revision",
            state_store_summary["current_revision"] == state_store_summary["head_revision"],
            {
                "current_revision": state_store_summary["current_revision"],
                "head_revision": state_store_summary["head_revision"],
            },
        ),
    ]
    check_errors = [entry["check"] for entry in checks if entry["status"] == "error"]
    if check_errors:
        raise ProtectedPilotPromotionError(
            "promotion sealing checks failed: " + ", ".join(check_errors)
        )

    manifest = {
        "project": "etl-identity-engine",
        "bundle_type": "protected_pilot_promotion",
        "manifest_version": PROMOTION_MANIFEST_VERSION,
        "status": "sealed",
        "promotion_label": resolved_label,
        "generated_at_utc": resolved_generated_at_utc,
        "source_commit": source_commit or _resolve_source_commit(),
        "pilot_name": pilot_name,
        "version": bundle_version,
        "environment": environment_name,
        "target_id": target_id,
        "tenant_id": tenant_id,
        "runtime": {
            "runtime_config_snapshot": RUNTIME_CONFIG_SNAPSHOT_NAME,
            "runtime_env_fingerprint_path": RUNTIME_ENV_FINGERPRINT_NAME,
            "environment_summary": runtime_summary,
            "state_store": state_store_summary,
        },
        "inputs": {
            "customer_pilot_bundle": {
                **bundle_summary["bundle_record"],
                "pilot_manifest_path": PILOT_MANIFEST_COPY_NAME,
                "handoff_manifest_path": PILOT_HANDOFF_MANIFEST_COPY_NAME,
                "handoff_signature_path": (
                    PILOT_HANDOFF_SIGNATURE_COPY_NAME
                    if bundle_summary["signature_bytes"] is not None
                    else None
                ),
                "trusted_public_key": bundle_summary["trusted_public_key"],
                "pilot_source_run_id": pilot_manifest.get("source_run_id"),
            },
            "custody_manifest": _input_reference(
                custody_manifest_path.resolve(),
                copied_relative_path=CUSTODY_MANIFEST_COPY_NAME,
                extra={
                    "target_id": copied_custody_manifest.get("target_id"),
                    "vendor_profile": copied_custody_manifest.get("vendor_profile"),
                    "batch_id": copied_custody_manifest.get("replay_linkage", {}).get("batch_id"),
                    "captured_at_utc": copied_custody_manifest.get("captured_at_utc"),
                },
            ),
            "acceptance_package_summary": _input_reference(
                acceptance_summary_path.resolve(),
                copied_relative_path=ACCEPTANCE_SUMMARY_COPY_NAME,
                extra={
                    "target_id": copied_acceptance_summary.get("target_id"),
                    "acceptance_root": copied_acceptance_summary.get("acceptance_root"),
                    "drift_report_path": copied_acceptance_summary.get("drift_report_path"),
                },
            ),
            "cjis_evidence_pack": _input_reference(
                evidence_pack_path.resolve(),
                copied_relative_path=EVIDENCE_MANIFEST_COPY_NAME,
                extra={
                    "environment": evidence_manifest.get("environment"),
                    "preflight_status": evidence_manifest.get("preflight_status"),
                    "selected_run_id": evidence_manifest.get("selected_run_id"),
                },
            ),
            "ha_rehearsal_summary": _input_reference(
                ha_rehearsal_summary_path.resolve(),
                copied_relative_path=HA_REHEARSAL_SUMMARY_COPY_NAME,
                extra={
                    "run_id": copied_ha_rehearsal_summary.get("run_id"),
                    "replay_run_id": copied_ha_rehearsal_summary.get("replay_run_id"),
                    "validated_steps": copied_ha_rehearsal_summary.get("validated_steps"),
                },
            ),
            "rollback_bundle": _input_reference(
                rollback_bundle_path.resolve(),
                extra={"artifact_role": "rollback_bundle"},
            ),
        },
        "checks": checks,
        "rollback": {
            "recorded_artifact": str(rollback_bundle_path.resolve()),
            "steps": _build_rollback_steps(rollback_bundle=rollback_bundle_path.resolve()),
        },
        "revalidation": {
            "commands": _build_revalidation_commands(
                environment_name=environment_name,
                runtime_config_path=resolved_runtime_config_path,
                env_file=resolved_env_file,
                bundle_path=None if bundle is None else bundle.resolve(),
                bundle_root=None if bundle_root is None else bundle_root.resolve(),
            ),
        },
    }
    manifest_path = promotion_root / PROMOTION_MANIFEST_NAME
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    manifest_path.write_bytes(manifest_bytes)
    summary_path = promotion_root / PROMOTION_SUMMARY_NAME
    summary_path.write_text(_build_summary_markdown(manifest), encoding="utf-8")

    signature_path: Path | None = None
    if signing_key is not None:
        signature_path = promotion_root / _signature_sidecar_name(PROMOTION_MANIFEST_NAME)
        _write_detached_signature(
            destination=signature_path,
            manifest_path=PROMOTION_MANIFEST_NAME,
            manifest_bytes=manifest_bytes,
            private_key_path=signing_key.resolve(),
            signer_identity=signer_identity,
            key_id=key_id,
        )

    return {
        "status": "sealed",
        "promotion_root": str(promotion_root),
        "promotion_manifest_path": str(manifest_path),
        "promotion_summary_path": str(summary_path),
        "promotion_manifest_signature_path": None if signature_path is None else str(signature_path),
        "promotion_label": resolved_label,
        "pilot_name": pilot_name,
        "target_id": target_id,
        "environment": environment_name,
        "manifest": manifest,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = seal_protected_pilot_promotion(
        output_dir=_resolve_output_dir(args.output_dir),
        bundle=None if args.bundle is None else Path(args.bundle),
        bundle_root=None if args.bundle_root is None else Path(args.bundle_root),
        trusted_public_key=None if args.trusted_public_key is None else Path(args.trusted_public_key),
        runtime_config_path=Path(args.runtime_config),
        environment_name=args.environment,
        env_file=Path(args.env_file),
        state_db=args.state_db,
        custody_manifest_path=Path(args.custody_manifest),
        acceptance_summary_path=Path(args.acceptance_summary),
        evidence_pack_path=Path(args.evidence_pack),
        ha_rehearsal_summary_path=Path(args.ha_rehearsal_summary),
        rollback_bundle_path=Path(args.rollback_bundle),
        promotion_label=args.promotion_label,
        signing_key=None if args.signing_key is None else Path(args.signing_key),
        signer_identity=args.signer_identity,
        key_id=args.key_id,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
