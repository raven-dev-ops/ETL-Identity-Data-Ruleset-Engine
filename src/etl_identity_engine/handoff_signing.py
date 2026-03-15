from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey


SIGNATURE_VERSION = "v1"
SIGNATURE_ALGORITHM = "ed25519"
SIGNATURE_SUFFIX = ".sig.json"


def signature_sidecar_name(manifest_name: str) -> str:
    if manifest_name.endswith(".json"):
        return f"{manifest_name[:-5]}{SIGNATURE_SUFFIX}"
    return f"{manifest_name}{SIGNATURE_SUFFIX}"


def public_key_fingerprint_sha256(public_key_bytes: bytes) -> str:
    return hashlib.sha256(public_key_bytes).hexdigest()


def _public_key_bytes(public_key: Ed25519PublicKey) -> bytes:
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def load_private_key(private_key_path: Path) -> Ed25519PrivateKey:
    private_key = serialization.load_pem_private_key(private_key_path.read_bytes(), password=None)
    if not isinstance(private_key, Ed25519PrivateKey):
        raise ValueError(f"Expected an Ed25519 private key in {private_key_path}")
    return private_key


def load_public_key(public_key_path: Path) -> Ed25519PublicKey:
    public_key = serialization.load_pem_public_key(public_key_path.read_bytes())
    if not isinstance(public_key, Ed25519PublicKey):
        raise ValueError(f"Expected an Ed25519 public key in {public_key_path}")
    return public_key


def create_detached_signature(
    *,
    manifest_path: str,
    manifest_bytes: bytes,
    private_key_path: Path,
    signer_identity: str | None = None,
    key_id: str | None = None,
) -> dict[str, str]:
    private_key = load_private_key(private_key_path)
    public_key_bytes = _public_key_bytes(private_key.public_key())
    fingerprint = public_key_fingerprint_sha256(public_key_bytes)
    return {
        "signature_version": SIGNATURE_VERSION,
        "manifest_path": manifest_path,
        "algorithm": SIGNATURE_ALGORITHM,
        "key_id": (key_id or fingerprint[:16]),
        "signer_identity": signer_identity or "unspecified",
        "public_key_fingerprint_sha256": fingerprint,
        "signature_base64": base64.b64encode(private_key.sign(manifest_bytes)).decode("ascii"),
    }


def write_detached_signature(
    *,
    destination: Path,
    manifest_path: str,
    manifest_bytes: bytes,
    private_key_path: Path,
    signer_identity: str | None = None,
    key_id: str | None = None,
) -> dict[str, str]:
    signature_payload = create_detached_signature(
        manifest_path=manifest_path,
        manifest_bytes=manifest_bytes,
        private_key_path=private_key_path,
        signer_identity=signer_identity,
        key_id=key_id,
    )
    destination.write_text(
        json.dumps(signature_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return signature_payload


def verify_detached_signature(
    *,
    manifest_path: str,
    manifest_bytes: bytes,
    signature_payload: Mapping[str, object],
    trusted_public_key_path: Path,
) -> dict[str, object]:
    signature_version = str(signature_payload.get("signature_version", "")).strip()
    if signature_version != SIGNATURE_VERSION:
        raise ValueError(
            f"Unsupported signature version {signature_version!r}; expected {SIGNATURE_VERSION!r}"
        )

    algorithm = str(signature_payload.get("algorithm", "")).strip().lower()
    if algorithm != SIGNATURE_ALGORITHM:
        raise ValueError(f"Unsupported signature algorithm {algorithm!r}; expected {SIGNATURE_ALGORITHM!r}")

    expected_manifest_path = str(signature_payload.get("manifest_path", "")).strip()
    if expected_manifest_path != manifest_path:
        raise ValueError(
            f"Detached signature targets {expected_manifest_path!r}, expected {manifest_path!r}"
        )

    public_key = load_public_key(trusted_public_key_path)
    trusted_public_key_bytes = _public_key_bytes(public_key)
    trusted_fingerprint = public_key_fingerprint_sha256(trusted_public_key_bytes)
    signature_fingerprint = str(signature_payload.get("public_key_fingerprint_sha256", "")).strip()
    if signature_fingerprint != trusted_fingerprint:
        raise ValueError(
            "Trusted public key fingerprint does not match detached signature fingerprint"
        )

    signature_base64 = str(signature_payload.get("signature_base64", "")).strip()
    if not signature_base64:
        raise ValueError("Detached signature payload is missing signature_base64")

    signature_bytes = base64.b64decode(signature_base64.encode("ascii"))
    try:
        public_key.verify(signature_bytes, manifest_bytes)
    except InvalidSignature as exc:
        raise ValueError("Detached signature verification failed") from exc

    return {
        "status": "ok",
        "signature_version": signature_version,
        "algorithm": algorithm,
        "manifest_path": expected_manifest_path,
        "key_id": str(signature_payload.get("key_id", "")).strip(),
        "signer_identity": str(signature_payload.get("signer_identity", "")).strip(),
        "trusted_public_key_fingerprint_sha256": trusted_fingerprint,
        "signature_public_key_fingerprint_sha256": signature_fingerprint,
    }
