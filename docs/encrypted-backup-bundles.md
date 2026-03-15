# Encrypted Backup Bundles

The repo now ships a shared encrypted-bundle format for two operator
workflows:

- persisted-state backup and restore
- encrypted customer-pilot handoff delivery

The format is a zip wrapper containing:

- `encrypted_bundle_manifest.json`
- `payload.bin`

The payload itself is an encrypted zip. The runtime supports two secret
types:

- passphrase-derived encryption with PBKDF2-SHA256
- a raw base64-encoded 32-byte AES key

## Persisted-State Backup

Create an encrypted persisted-state bundle:

```bash
python -m etl_identity_engine.cli backup-state-bundle \
  --state-db data/state/pipeline_state.sqlite \
  --output dist/state-backups/pipeline_state_backup_encrypted.zip \
  --include-path data/replay_bundles/RUN-20260315T000000Z-EXAMPLE \
  --passphrase-file C:\secrets\state-backup-passphrase.txt
```

Supported secret inputs are:

- `--passphrase-env`
- `--passphrase-file`
- `--key-env`
- `--key-file`

The encrypted bundle contains:

- exported state tables under `state_export/tables/*.jsonl`
- `state_export/state_backup_manifest.json`
- any requested attachment files or directories under `attachments/`

Restore that encrypted bundle into a target state store:

```bash
python -m etl_identity_engine.cli restore-state-bundle \
  --state-db recovery/state/pipeline_state.sqlite \
  --bundle dist/state-backups/pipeline_state_backup_encrypted.zip \
  --attachments-output-dir recovery/replay_bundles \
  --passphrase-file C:\secrets\state-backup-passphrase.txt
```

Use `--replace-existing` only when the target state store already
contains rows that should be overwritten.

## Generic Decrypt And Extract

For encrypted handoff bundles that should be unpacked as files first,
use the generic extractor:

```bash
python scripts/restore_encrypted_bundle.py \
  --bundle dist/customer-pilot/etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions-encrypted.zip \
  --output-dir dist/customer-pilot/extracted \
  --passphrase-file C:\secrets\pilot-bundle-passphrase.txt
```

That command only decrypts and extracts the payload. It does not import
state into a database.

## Encrypted Customer Pilot Delivery

`package_customer_pilot_bundle.py` can now wrap the standalone pilot
handoff in the same encrypted format:

```bash
python scripts/package_customer_pilot_bundle.py \
  --output-dir dist/customer-pilot \
  --passphrase-file C:\secrets\pilot-bundle-passphrase.txt
```

That writes a bundle name like:

`etl-identity-engine-vX.Y.Z-customer-pilot-public-safety-regressions-encrypted.zip`

Recipients should decrypt it with `scripts/restore_encrypted_bundle.py`
before running the readiness check or bootstrap flow.
