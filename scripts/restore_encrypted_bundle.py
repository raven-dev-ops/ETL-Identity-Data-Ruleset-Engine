from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"


def _load_encrypted_bundle_helpers():
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
    from etl_identity_engine.encrypted_bundle import extract_encrypted_bundle, resolve_encryption_secret

    return extract_encrypted_bundle, resolve_encryption_secret


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decrypt and extract an encrypted ETL Identity Engine bundle."
    )
    parser.add_argument("--bundle", required=True, help="Encrypted bundle zip to extract.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where the decrypted payload should be extracted.",
    )
    secret_group = parser.add_mutually_exclusive_group(required=True)
    secret_group.add_argument(
        "--passphrase-env",
        default=None,
        help="Environment variable containing the decryption passphrase.",
    )
    secret_group.add_argument(
        "--passphrase-file",
        default=None,
        help="File containing the decryption passphrase.",
    )
    secret_group.add_argument(
        "--key-env",
        default=None,
        help="Environment variable containing a base64-encoded 32-byte AES key.",
    )
    secret_group.add_argument(
        "--key-file",
        default=None,
        help="File containing a base64-encoded 32-byte AES key.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    extract_encrypted_bundle, resolve_encryption_secret = _load_encrypted_bundle_helpers()
    secret = resolve_encryption_secret(
        passphrase_env=args.passphrase_env,
        passphrase_file=None if args.passphrase_file is None else Path(args.passphrase_file).resolve(),
        key_env=args.key_env,
        key_file=None if args.key_file is None else Path(args.key_file).resolve(),
    )
    summary = extract_encrypted_bundle(
        bundle_path=Path(args.bundle).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        encryption_secret=secret,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
