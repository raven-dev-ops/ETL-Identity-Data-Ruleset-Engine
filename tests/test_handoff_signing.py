from __future__ import annotations

from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from etl_identity_engine.handoff_signing import (
    create_detached_signature,
    signature_sidecar_name,
    verify_detached_signature,
)


def _write_ed25519_keypair(root: Path) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    private_key_path = root / "signing-private.pem"
    public_key_path = root / "signing-public.pem"
    private_key_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    public_key_path.write_bytes(
        public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    return private_key_path, public_key_path


def test_signature_sidecar_name_uses_expected_suffix() -> None:
    assert signature_sidecar_name("manifest.json") == "manifest.sig.json"
    assert signature_sidecar_name("pilot_handoff_manifest.json") == "pilot_handoff_manifest.sig.json"


def test_verify_detached_signature_accepts_matching_ed25519_keypair(tmp_path: Path) -> None:
    private_key_path, public_key_path = _write_ed25519_keypair(tmp_path)
    manifest_bytes = b'{"project":"etl-identity-engine"}\n'
    signature_payload = create_detached_signature(
        manifest_path="manifest.json",
        manifest_bytes=manifest_bytes,
        private_key_path=private_key_path,
        signer_identity="release-bot@example.test",
        key_id="release-ed25519",
    )

    verification = verify_detached_signature(
        manifest_path="manifest.json",
        manifest_bytes=manifest_bytes,
        signature_payload=signature_payload,
        trusted_public_key_path=public_key_path,
    )

    assert verification["status"] == "ok"
    assert verification["key_id"] == "release-ed25519"
    assert verification["signer_identity"] == "release-bot@example.test"


def test_verify_detached_signature_rejects_wrong_public_key(tmp_path: Path) -> None:
    private_key_path, _ = _write_ed25519_keypair(tmp_path / "correct")
    _, wrong_public_key_path = _write_ed25519_keypair(tmp_path / "wrong")
    manifest_bytes = b'{"project":"etl-identity-engine"}\n'
    signature_payload = create_detached_signature(
        manifest_path="manifest.json",
        manifest_bytes=manifest_bytes,
        private_key_path=private_key_path,
    )

    try:
        verify_detached_signature(
            manifest_path="manifest.json",
            manifest_bytes=manifest_bytes,
            signature_payload=signature_payload,
            trusted_public_key_path=wrong_public_key_path,
        )
    except ValueError as exc:
        assert "fingerprint" in str(exc)
    else:
        raise AssertionError("expected detached signature verification to fail")
