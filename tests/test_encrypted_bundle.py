from __future__ import annotations

import base64
import importlib.util
from pathlib import Path
import sys
import tempfile

import pytest

from etl_identity_engine.encrypted_bundle import (
    create_encrypted_bundle,
    extract_encrypted_bundle,
    resolve_encryption_secret,
)


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "restore_encrypted_bundle.py"
SPEC = importlib.util.spec_from_file_location("restore_encrypted_bundle_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_create_and_extract_encrypted_bundle_round_trip(tmp_path: Path) -> None:
    staging_root = tmp_path / "staging"
    (staging_root / "nested").mkdir(parents=True)
    (staging_root / "manifest.json").write_text('{"bundle":"demo"}\n', encoding="utf-8")
    (staging_root / "nested" / "payload.txt").write_text("hello\n", encoding="utf-8")

    encryption_secret = resolve_encryption_secret(
        passphrase_env="ETL_TEST_PASSPHRASE",
        environ={"ETL_TEST_PASSPHRASE": "encrypted-bundle-secret"},
    )
    bundle_path = tmp_path / "encrypted-demo.zip"

    create_encrypted_bundle(
        staging_root=staging_root,
        destination=bundle_path,
        bundle_type="demo_fixture",
        encryption_secret=encryption_secret,
        generated_at_utc="2026-03-15T00:00:00Z",
        metadata={"fixture": True},
    )

    extracted_root = tmp_path / "extracted"
    summary = extract_encrypted_bundle(
        bundle_path=bundle_path,
        output_dir=extracted_root,
        encryption_secret=encryption_secret,
    )

    assert summary["bundle_type"] == "demo_fixture"
    assert summary["generated_at_utc"] == "2026-03-15T00:00:00Z"
    assert (extracted_root / "manifest.json").read_text(encoding="utf-8") == '{"bundle":"demo"}\n'
    assert (extracted_root / "nested" / "payload.txt").read_text(encoding="utf-8") == "hello\n"


def test_resolve_encryption_secret_rejects_invalid_raw_key() -> None:
    with pytest.raises(ValueError, match="base64-encoded"):
        resolve_encryption_secret(key_env="ETL_TEST_RAW_KEY", environ={"ETL_TEST_RAW_KEY": "not-base64"})

    too_short_key = base64.b64encode(b"short-key").decode("ascii")
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        resolve_encryption_secret(key_env="ETL_TEST_RAW_KEY", environ={"ETL_TEST_RAW_KEY": too_short_key})


def test_restore_encrypted_bundle_script_extracts_bundle(tmp_path: Path) -> None:
    staging_root = tmp_path / "staging"
    staging_root.mkdir(parents=True)
    (staging_root / "payload.txt").write_text("script-path\n", encoding="utf-8")
    passphrase_file = tmp_path / "bundle-passphrase.txt"
    passphrase_file.write_text("script-secret\n", encoding="utf-8")
    secret = resolve_encryption_secret(passphrase_file=passphrase_file)
    bundle_path = tmp_path / "bundle.zip"

    create_encrypted_bundle(
        staging_root=staging_root,
        destination=bundle_path,
        bundle_type="script_fixture",
        encryption_secret=secret,
    )

    with tempfile.TemporaryDirectory(prefix="encrypted-bundle-script-") as temp_dir:
        output_dir = Path(temp_dir) / "output"
        assert (
            MODULE.main(
                [
                    "--bundle",
                    str(bundle_path),
                    "--output-dir",
                    str(output_dir),
                    "--passphrase-file",
                    str(passphrase_file),
                ]
            )
            == 0
        )
        assert (output_dir / "payload.txt").read_text(encoding="utf-8") == "script-path\n"
