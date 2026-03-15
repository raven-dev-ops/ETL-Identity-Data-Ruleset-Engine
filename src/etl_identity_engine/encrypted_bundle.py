"""Shared encrypted bundle helpers for portable backup and handoff artifacts."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import zipfile

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


ENCRYPTED_BUNDLE_VERSION = "v1"
ENCRYPTED_BUNDLE_MANIFEST_NAME = "encrypted_bundle_manifest.json"
ENCRYPTED_BUNDLE_PAYLOAD_NAME = "payload.bin"
PBKDF2_ITERATIONS = 600_000
AES_KEY_BYTES = 32
NONCE_BYTES = 12
SALT_BYTES = 16


@dataclass(frozen=True)
class EncryptionSecret:
    mode: str
    secret_bytes: bytes
    source: str


def resolve_encryption_secret(
    *,
    passphrase_env: str | None = None,
    passphrase_file: Path | None = None,
    key_env: str | None = None,
    key_file: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> EncryptionSecret:
    selected = [
        ("passphrase_env", passphrase_env is not None),
        ("passphrase_file", passphrase_file is not None),
        ("key_env", key_env is not None),
        ("key_file", key_file is not None),
    ]
    configured = [name for name, present in selected if present]
    if len(configured) != 1:
        raise ValueError(
            "Exactly one of passphrase_env, passphrase_file, key_env, or key_file must be provided"
        )

    effective_environ = os.environ if environ is None else environ
    if passphrase_env is not None:
        raw_value = str(effective_environ.get(passphrase_env, "") or "")
        if not raw_value.strip():
            raise ValueError(f"Passphrase environment variable {passphrase_env} is not set")
        return EncryptionSecret(
            mode="passphrase",
            secret_bytes=raw_value.encode("utf-8"),
            source=f"env:{passphrase_env}",
        )
    if passphrase_file is not None:
        payload = passphrase_file.read_text(encoding="utf-8").strip()
        if not payload:
            raise ValueError(f"Passphrase file is empty: {passphrase_file}")
        return EncryptionSecret(
            mode="passphrase",
            secret_bytes=payload.encode("utf-8"),
            source=f"file:{passphrase_file}",
        )
    if key_env is not None:
        raw_value = str(effective_environ.get(key_env, "") or "").strip()
        if not raw_value:
            raise ValueError(f"Key environment variable {key_env} is not set")
        return EncryptionSecret(
            mode="raw_key",
            secret_bytes=_decode_base64_key(raw_value, source=f"env:{key_env}"),
            source=f"env:{key_env}",
        )
    if key_file is not None:
        raw_value = key_file.read_text(encoding="utf-8").strip()
        if not raw_value:
            raise ValueError(f"Key file is empty: {key_file}")
        return EncryptionSecret(
            mode="raw_key",
            secret_bytes=_decode_base64_key(raw_value, source=f"file:{key_file}"),
            source=f"file:{key_file}",
        )
    raise AssertionError("unreachable")


def create_encrypted_bundle(
    *,
    staging_root: Path,
    destination: Path,
    bundle_type: str,
    encryption_secret: EncryptionSecret,
    generated_at_utc: str | None = None,
    metadata: Mapping[str, object] | None = None,
) -> Path:
    staging_root = staging_root.resolve()
    if not staging_root.exists() or not staging_root.is_dir():
        raise FileNotFoundError(f"Encrypted bundle staging root not found: {staging_root}")

    payload_bytes = _zip_directory_bytes(staging_root)
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    bundle_generated_at = generated_at_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    salt = os.urandom(SALT_BYTES)
    nonce = os.urandom(NONCE_BYTES)
    derived_key, encryption_metadata = _derive_encryption_key(
        encryption_secret=encryption_secret,
        salt=salt,
    )
    ciphertext = AESGCM(derived_key).encrypt(nonce, payload_bytes, None)

    manifest_payload: dict[str, object] = {
        "bundle_version": ENCRYPTED_BUNDLE_VERSION,
        "bundle_type": bundle_type,
        "generated_at_utc": bundle_generated_at,
        "payload_name": ENCRYPTED_BUNDLE_PAYLOAD_NAME,
        "payload_sha256": hashlib.sha256(ciphertext).hexdigest(),
        "plaintext_sha256": payload_sha256,
        "plaintext_entries": [
            str(path.relative_to(staging_root)).replace("\\", "/")
            for path in sorted(staging_root.rglob("*"))
            if path.is_file()
        ],
        "encryption": {
            "algorithm": "aes-256-gcm",
            "nonce_base64": base64.b64encode(nonce).decode("ascii"),
            **encryption_metadata,
        },
    }
    if metadata:
        manifest_payload["metadata"] = dict(metadata)

    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        manifest_bytes = (json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
        archive.writestr(ENCRYPTED_BUNDLE_MANIFEST_NAME, manifest_bytes)
        archive.writestr(ENCRYPTED_BUNDLE_PAYLOAD_NAME, ciphertext)
    return destination


def extract_encrypted_bundle(
    *,
    bundle_path: Path,
    output_dir: Path,
    encryption_secret: EncryptionSecret,
) -> dict[str, object]:
    bundle_path = bundle_path.resolve()
    if not bundle_path.exists():
        raise FileNotFoundError(f"Encrypted bundle not found: {bundle_path}")

    with zipfile.ZipFile(bundle_path) as archive:
        manifest_payload = json.loads(archive.read(ENCRYPTED_BUNDLE_MANIFEST_NAME).decode("utf-8"))
        ciphertext = archive.read(str(manifest_payload.get("payload_name", ENCRYPTED_BUNDLE_PAYLOAD_NAME)))

    expected_payload_sha256 = str(manifest_payload.get("payload_sha256", "") or "")
    actual_payload_sha256 = hashlib.sha256(ciphertext).hexdigest()
    if expected_payload_sha256 != actual_payload_sha256:
        raise ValueError("Encrypted bundle payload hash does not match the bundle manifest")

    encryption_payload = manifest_payload.get("encryption")
    if not isinstance(encryption_payload, Mapping):
        raise ValueError("Encrypted bundle manifest is missing encryption metadata")
    nonce = base64.b64decode(str(encryption_payload.get("nonce_base64", "") or ""))
    derived_key = _derive_decryption_key(
        encryption_secret=encryption_secret,
        encryption_payload=encryption_payload,
    )
    plaintext_zip = AESGCM(derived_key).decrypt(nonce, ciphertext, None)
    expected_plaintext_sha256 = str(manifest_payload.get("plaintext_sha256", "") or "")
    actual_plaintext_sha256 = hashlib.sha256(plaintext_zip).hexdigest()
    if expected_plaintext_sha256 != actual_plaintext_sha256:
        raise ValueError("Decrypted bundle payload hash does not match the bundle manifest")

    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="etl-identity-engine-encrypted-bundle-") as temp_dir:
        temp_zip = Path(temp_dir) / "payload.zip"
        temp_zip.write_bytes(plaintext_zip)
        with zipfile.ZipFile(temp_zip) as archive:
            archive.extractall(output_dir)

    return {
        "bundle_path": str(bundle_path),
        "output_dir": str(output_dir.resolve()),
        "bundle_type": str(manifest_payload.get("bundle_type", "") or ""),
        "generated_at_utc": str(manifest_payload.get("generated_at_utc", "") or ""),
        "plaintext_entries": list(manifest_payload.get("plaintext_entries", [])),
    }


def _zip_directory_bytes(root: Path) -> bytes:
    with tempfile.NamedTemporaryFile(prefix="etl-identity-engine-payload-", suffix=".zip", delete=False) as handle:
        temp_zip_path = Path(handle.name)
    try:
        with zipfile.ZipFile(temp_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for source_path in sorted(root.rglob("*")):
                if not source_path.is_file():
                    continue
                relative_path = source_path.relative_to(root).as_posix()
                zip_info = zipfile.ZipInfo(relative_path)
                zip_info.compress_type = zipfile.ZIP_DEFLATED
                zip_info.external_attr = 0o100644 << 16
                archive.writestr(zip_info, source_path.read_bytes())
        return temp_zip_path.read_bytes()
    finally:
        if temp_zip_path.exists():
            temp_zip_path.unlink()


def _derive_encryption_key(
    *,
    encryption_secret: EncryptionSecret,
    salt: bytes,
) -> tuple[bytes, dict[str, object]]:
    if encryption_secret.mode == "raw_key":
        return encryption_secret.secret_bytes, {
            "secret_mode": "raw_key",
            "kdf": "none",
        }
    if encryption_secret.mode == "passphrase":
        derived_key = _pbkdf2_key(encryption_secret.secret_bytes, salt=salt)
        return derived_key, {
            "secret_mode": "passphrase",
            "kdf": "pbkdf2_sha256",
            "iterations": PBKDF2_ITERATIONS,
            "salt_base64": base64.b64encode(salt).decode("ascii"),
        }
    raise ValueError(f"Unsupported encryption secret mode: {encryption_secret.mode}")


def _derive_decryption_key(
    *,
    encryption_secret: EncryptionSecret,
    encryption_payload: Mapping[str, object],
) -> bytes:
    secret_mode = str(encryption_payload.get("secret_mode", "") or "")
    if secret_mode != encryption_secret.mode:
        raise ValueError(
            f"Encrypted bundle expects secret mode {secret_mode!r}, received {encryption_secret.mode!r}"
        )
    if secret_mode == "raw_key":
        return encryption_secret.secret_bytes
    if secret_mode == "passphrase":
        salt = base64.b64decode(str(encryption_payload.get("salt_base64", "") or ""))
        return _pbkdf2_key(encryption_secret.secret_bytes, salt=salt)
    raise ValueError(f"Unsupported encrypted bundle secret mode: {secret_mode!r}")


def _pbkdf2_key(passphrase: bytes, *, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=AES_KEY_BYTES,
        salt=salt,
        iterations=PBKDF2_ITERATIONS,
    )
    return kdf.derive(passphrase)


def _decode_base64_key(raw_value: str, *, source: str) -> bytes:
    try:
        decoded = base64.b64decode(raw_value, validate=True)
    except (ValueError, binascii.Error):  # type: ignore[name-defined]
        raise ValueError(f"Raw encryption key from {source} must be base64-encoded")
    if len(decoded) != AES_KEY_BYTES:
        raise ValueError(
            f"Raw encryption key from {source} must decode to exactly {AES_KEY_BYTES} bytes"
        )
    return decoded
