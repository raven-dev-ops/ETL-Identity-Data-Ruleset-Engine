from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_handoff_signature.py"
SPEC = importlib.util.spec_from_file_location("verify_handoff_signature_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


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


def _write_signed_customer_bundle_root(root: Path, *, private_key_path: Path) -> None:
    files = {
        "README.md": "# demo\n",
        "runtime/requirements-pilot.txt": "Django>=5.2,<5.3\n",
        "runtime/config/runtime_environments.yml": "default_environment: container\nenvironments: {}\n",
        "launch/bootstrap_windows_pilot.ps1": "Write-Host bootstrap\n",
        "launch/check_pilot_readiness.ps1": "Write-Host readiness\n",
        "tools/bootstrap_windows_pilot.py": "print('bootstrap')\n",
        "tools/check_pilot_readiness.py": "print('readiness')\n",
        "state/pipeline_state.sqlite": "sqlite\n",
        "pilot_manifest.json": json.dumps(
            {
                "project": "etl-identity-engine",
                "bundle_type": "customer_pilot",
                "version": "1.0.0",
                "pilot_name": "public-safety-regressions",
                "generated_at_utc": "2026-03-15T00:00:00Z",
                "source_commit": "abc123",
                "source_manifest": "seed_dataset/manifest.yml",
                "source_run_id": "RUN-EXAMPLE",
                "state_db": "state/pipeline_state.sqlite",
                "demo_shell_dir": "demo_shell",
                "launch_helpers": ["launch/bootstrap_windows_pilot.ps1"],
                "artifacts": ["README.md", "pilot_handoff_manifest.json", "pilot_handoff_manifest.sig.json"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    }
    for relative_path, contents in files.items():
        path = root.joinpath(*relative_path.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")

    artifact_entries = []
    for relative_path in sorted(files):
        path = root.joinpath(*relative_path.split("/"))
        payload = path.read_bytes()
        artifact_entries.append(
            {
                "path": relative_path,
                "sha256": MODULE._sha256_bytes(payload),
                "size_bytes": len(payload),
            }
        )
    handoff_manifest_path = root / "pilot_handoff_manifest.json"
    handoff_manifest_bytes = (
        json.dumps(
            {
                "project": "etl-identity-engine",
                "bundle_type": "customer_pilot",
                "version": "1.0.0",
                "pilot_name": "public-safety-regressions",
                "generated_at_utc": "2026-03-15T00:00:00Z",
                "source_commit": "abc123",
                "source_manifest": "seed_dataset/manifest.yml",
                "source_run_id": "RUN-EXAMPLE",
                "verification_type": "sha256",
                "artifacts": artifact_entries,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    handoff_manifest_path.write_bytes(handoff_manifest_bytes)

    from etl_identity_engine.handoff_signing import write_detached_signature

    write_detached_signature(
        destination=root / "pilot_handoff_manifest.sig.json",
        manifest_path="pilot_handoff_manifest.json",
        manifest_bytes=handoff_manifest_bytes,
        private_key_path=private_key_path,
        signer_identity="pilot-signer@example.test",
        key_id="pilot-ed25519",
    )


def test_verify_handoff_signature_accepts_signed_customer_bundle_root(tmp_path: Path) -> None:
    private_key_path, public_key_path = _write_ed25519_keypair(tmp_path / "keys")
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    _write_signed_customer_bundle_root(bundle_root, private_key_path=private_key_path)

    summary = MODULE.verify_handoff_signature(
        bundle_root=str(bundle_root),
        trusted_public_key=str(public_key_path),
    )

    assert summary["status"] == "ok"
    assert summary["manifest_path"] == "pilot_handoff_manifest.json"
    assert summary["signature"]["key_id"] == "pilot-ed25519"
    assert any(check["check"] == "artifact:README.md" for check in summary["checks"])


def test_verify_handoff_signature_reports_hash_tampering(tmp_path: Path) -> None:
    private_key_path, public_key_path = _write_ed25519_keypair(tmp_path / "keys")
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    _write_signed_customer_bundle_root(bundle_root, private_key_path=private_key_path)
    (bundle_root / "README.md").write_text("# tampered\n", encoding="utf-8")

    summary = MODULE.verify_handoff_signature(
        bundle_root=str(bundle_root),
        trusted_public_key=str(public_key_path),
    )

    assert summary["status"] == "error"
    assert any("README.md" in error for error in summary["errors"])
