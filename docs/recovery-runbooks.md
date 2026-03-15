# Recovery Runbooks

This runbook defines the supported backup, restore, and replay model
for persisted SQL-backed runs.

## Recovery Model

Persisted run state is split across two layers:

- the persisted state store, which stores run registry rows, normalized
  rows, candidate decisions, clusters, golden records, crosswalk rows,
  review cases, export history, and audit events
- the archived replay bundle for a manifest-driven run, which stores
  the original manifest plus the landed source snapshot needed for
  recovery

That distinction matters operationally:

- `report`, `publish-run`, and `export-job-run` can rebuild downstream
  outputs from the restored persisted state store alone
- `replay-run` can now execute directly from the verified archived
  replay bundle without restoring the original manifest and landing
  paths

The runtime now archives, verifies, and replays from replay bundles.
Recovery therefore means backing up the persisted state database plus
the replay bundle through the encrypted backup workflow, then restoring
that bundle before using `replay-run`.

## Minimum Backup Set

For each manifest-driven persisted batch that must be recoverable, back
up:

- the SQLite state database, for example
  `data/state/pipeline_state.sqlite`
- the replay bundle referenced by `run_summary.json` and
  `summary.replay_bundle`
- any custom runtime config directory used for that run if it differs
  from the committed repo `config/` files

If you already publish delivery snapshots for consumers, keep those
snapshots as separate downstream backups. They are useful for consumer
continuity, but they are not a substitute for the persisted state DB and
manifest-era inputs when operators need replay.

## Backup Procedure

1. Quiesce new writes for the target state DB. The safest point is after
   a batch completes and before the next persisted write begins.
2. Run replay-bundle verification for the target run:

```bash
python -m etl_identity_engine.cli verify-replay-bundle \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345
```

3. Export the encrypted backup bundle:

```bash
python -m etl_identity_engine.cli backup-state-bundle \
  --state-db data/state/pipeline_state.sqlite \
  --output recovery/pipeline_state_backup_encrypted.zip \
  --include-path data/replay_bundles/RUN-20260314T000000Z-ABC12345 \
  --passphrase-file C:\secrets\state-backup-passphrase.txt
```

4. If the run used a custom `--config-dir` or runtime environment
   overlay, either include that path with an additional `--include-path`
   or back it up separately.
5. Record the selected `run_id`, `batch_id`, and backup timestamp with
   the backup set.

## Restore Procedure

1. Restore the encrypted bundle to the target runtime location:

```bash
python -m etl_identity_engine.cli restore-state-bundle \
  --state-db data/state/pipeline_state.sqlite \
  --bundle recovery/pipeline_state_backup_encrypted.zip \
  --attachments-output-dir data/replay_bundles \
  --passphrase-file C:\secrets\state-backup-passphrase.txt
```

2. If the original run used a custom config overlay, restore that config
   snapshot before replaying.
3. Verify the restored DB schema is readable:

```bash
python -m etl_identity_engine.cli state-db-current \
  --state-db data/state/pipeline_state.sqlite
```

4. Verify review state is present for the target run when applicable:

```bash
python -m etl_identity_engine.cli review-case-list \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345
```

## Rebuild Outputs From Restored State

These commands do not require the landing snapshot once the SQLite DB is
restored:

```bash
python -m etl_identity_engine.cli report \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --output recovery/run_report.md

python -m etl_identity_engine.cli publish-run \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --output-dir recovery/published
```

Use that path when you only need to reconstruct downstream artifacts for
an already-completed run.

## Replay Procedure

Use replay when you need a new completed run based on restored review
decisions plus the archived replay bundle.

1. Restore the SQLite DB, the archived replay bundle, and any custom
   config snapshot.
2. Run `replay-run` against the restored state DB:

```bash
python -m etl_identity_engine.cli replay-run \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --base-dir recovery/replayed-run \
  --refresh-mode incremental
```

3. Validate the new run result:
   - the JSON payload returns a new `result_run_id`
   - the new run summary shows the expected `refresh_mode`
   - the run summary records `replayable_from_bundle: true`
   - approved and rejected review decisions carried forward into the
     replayed candidate decisions and golden rebuilds

For manifest-driven reruns, approved review decisions force merge and
rejected review decisions block merge before cluster and golden rebuilds
are persisted.

## Container Deployment Notes

For the compose topology in [container-deployment.md](container-deployment.md):

- back up the mounted runtime path that contains the SQLite DB
- back up the verified replay bundle for the manifest-driven batch
- restore the archived replay bundle to the same container-visible
  bundle path before using `replay-run`

The container baseline remains single-host. Recovery is file- and
volume-oriented rather than orchestrator-managed.

## Smoke Validation

The repo includes an executable recovery smoke path:

```bash
python scripts/persisted_state_recovery_smoke.py
```

That smoke path validates:

- verification plus encrypted backup of persisted state and the archived replay bundle
- restore of persisted review state
- report rebuild from restored SQLite state
- replay of a restored manifest-driven run directly from the archived
  replay bundle with an approved review
  decision applied during the recovered rebuild
