# Recovery Runbooks

This runbook defines the supported backup, restore, and replay model
for persisted SQL-backed runs.

## Recovery Model

Persisted run state is split across two layers:

- the persisted state store, which stores run registry rows, normalized
  rows, candidate decisions, clusters, golden records, crosswalk rows,
  review cases, export history, and audit events
- the manifest plus landed source snapshot referenced by a manifest-
  driven run

That distinction matters operationally:

- `report`, `publish-run`, and `export-job-run` can rebuild downstream
  outputs from the restored persisted state store alone
- `replay-run` requires the stored `manifest_path` to exist again, and
  the landed files referenced by that manifest must also be available

The current runtime does not yet provide immutable replay independent of
the stored manifest path and landing-zone contents. Recovery therefore
means backing up both the persisted state database and the manifest-era
input snapshot.

## Minimum Backup Set

For each manifest-driven persisted batch that must be recoverable, back
up:

- the SQLite state database, for example
  `data/state/pipeline_state.sqlite`
- the manifest file referenced by `pipeline_runs.manifest_path`
- the landed input snapshot referenced by that manifest
- any custom runtime config directory used for that run if it differs
  from the committed repo `config/` files

If you already publish delivery snapshots for consumers, keep those
snapshots as separate downstream backups. They are useful for consumer
continuity, but they are not a substitute for the persisted state DB and
manifest-era inputs when operators need replay.

## Backup Procedure

1. Quiesce new writes for the target state DB. The safest point is after
   a batch completes and before the next persisted write begins.
2. Copy the SQLite DB file to a backup location.
3. Copy the manifest file stored in the completed run record.
4. Copy the landed input files referenced by that manifest as one
   snapshot.
5. If the run used a custom `--config-dir` or runtime environment
   overlay, copy that config snapshot too.
6. Record the selected `run_id`, `batch_id`, and backup timestamp with
   the backup set.

## Restore Procedure

1. Restore the SQLite DB to the target runtime location.
2. Restore the manifest file to the same absolute path recorded in the
   persisted run when replay is required.
3. Restore the landed input snapshot to the locations referenced by the
   restored manifest.
4. If the original run used a custom config overlay, restore that config
   snapshot before replaying.
5. Verify the restored DB schema is readable:

```bash
python -m etl_identity_engine.cli state-db-current \
  --state-db data/state/pipeline_state.sqlite
```

6. Verify review state is present for the target run when applicable:

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
decisions plus the restored manifest-era inputs.

1. Restore the SQLite DB, manifest, landed input snapshot, and any
   custom config snapshot.
2. If the replay should create a distinct completed run, update the
   restored manifest `batch_id` to a new value.
3. Run `replay-run` against the restored state DB:

```bash
python -m etl_identity_engine.cli replay-run \
  --state-db data/state/pipeline_state.sqlite \
  --run-id RUN-20260314T000000Z-ABC12345 \
  --base-dir recovery/replayed-run \
  --refresh-mode incremental
```

4. Validate the new run result:
   - the JSON payload returns a new `result_run_id`
   - the new run summary shows the expected `refresh_mode`
   - approved and rejected review decisions carried forward into the
     replayed candidate decisions and golden rebuilds

For manifest-driven reruns, approved review decisions force merge and
rejected review decisions block merge before cluster and golden rebuilds
are persisted.

## Container Deployment Notes

For the compose topology in [container-deployment.md](container-deployment.md):

- back up the mounted runtime path that contains the SQLite DB
- back up the manifest and landed input snapshot mounted for the batch
  run
- restore the manifest and landed files to the same container-visible
  paths before using `replay-run`

The container baseline remains single-host. Recovery is file- and
volume-oriented rather than orchestrator-managed.

## Smoke Validation

The repo includes an executable recovery smoke path:

```bash
python scripts/persisted_state_recovery_smoke.py
```

That smoke path validates:

- backup of SQLite state plus manifest-era landed inputs
- restore of persisted review state
- report rebuild from restored SQLite state
- replay of a restored manifest-driven run with an approved review
  decision applied during the recovered rebuild
