"""Shared operator workflow actions used by CLI and service surfaces."""

from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Callable, Sequence

from etl_identity_engine.ingest.manifest import peek_manifest_batch_id
from etl_identity_engine.storage.sqlite_store import (
    PersistedReviewCase,
    PipelineRunRecord,
    SQLitePipelineStore,
    build_run_key,
)


CommandRunner = Callable[[Sequence[str] | None], int]


@dataclass(frozen=True)
class ReviewDecisionOperationResult:
    action: str
    case: PersistedReviewCase


@dataclass(frozen=True)
class ReplayRunOperationResult:
    action: str
    requested_run: PipelineRunRecord
    result_run: PipelineRunRecord
    state_db: Path
    base_dir: Path
    refresh_mode: str
    replay_command: tuple[str, ...]


def resolve_completed_run_id(store: SQLitePipelineStore, requested_run_id: str | None) -> str:
    run_id = requested_run_id or store.latest_completed_run_id()
    if run_id is None:
        raise FileNotFoundError(f"No completed persisted runs found in {store.db_path}")
    return run_id


def resolve_review_case_run_id(store: SQLitePipelineStore, requested_run_id: str | None) -> str:
    run_id = requested_run_id or store.latest_completed_run_id_with_review_cases()
    if run_id is None:
        raise FileNotFoundError(f"No completed persisted review-case runs found in {store.db_path}")
    return run_id


def apply_review_decision_operation(
    *,
    store: SQLitePipelineStore,
    run_id: str | None,
    review_id: str,
    decision: str,
    assigned_to: str | None,
    notes: str | None,
) -> ReviewDecisionOperationResult:
    resolved_run_id = resolve_review_case_run_id(store, run_id)
    existing = store.load_review_case(run_id=resolved_run_id, review_id=review_id)

    target_assigned_to = existing.assigned_to if assigned_to is None else assigned_to.strip()
    target_notes = existing.operator_notes if notes is None else notes.strip()
    is_noop = (
        existing.queue_status == decision
        and existing.assigned_to == target_assigned_to
        and existing.operator_notes == target_notes
    )
    if is_noop:
        return ReviewDecisionOperationResult(action="noop", case=existing)

    updated = store.update_review_case(
        run_id=resolved_run_id,
        review_id=review_id,
        queue_status=decision,
        assigned_to=assigned_to,
        operator_notes=notes,
    )
    return ReviewDecisionOperationResult(action="updated", case=updated)


def replay_run_operation(
    *,
    store: SQLitePipelineStore,
    state_db: Path,
    source_run_id: str | None,
    base_dir: Path | None,
    refresh_mode: str | None,
    runner: CommandRunner,
) -> ReplayRunOperationResult:
    resolved_source_run_id = resolve_completed_run_id(store, source_run_id)
    source_run = store.load_run_record(resolved_source_run_id)

    if source_run.input_mode != "manifest":
        raise ValueError(
            f"replay-run currently supports persisted manifest runs only; "
            f"run_id={source_run.run_id} input_mode={source_run.input_mode!r}"
        )
    if not source_run.manifest_path:
        raise ValueError(f"Persisted run {source_run.run_id} is missing manifest_path and cannot be replayed")

    manifest_path = Path(source_run.manifest_path)
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Cannot replay run_id={source_run.run_id} because manifest_path no longer exists: {manifest_path}"
        )

    resolved_refresh_mode = refresh_mode or str(
        source_run.summary.get("run_context", {}).get("refresh_mode", "full")
    )
    replay_base_dir = base_dir if base_dir is not None else Path(source_run.base_dir)
    resolved_manifest_path = str(manifest_path.resolve())
    run_key = build_run_key(
        input_mode="manifest",
        manifest_path=resolved_manifest_path,
        batch_id=peek_manifest_batch_id(manifest_path),
        config_dir=source_run.config_dir,
        profile=None,
        seed=None,
        person_count=None,
        duplicate_rate=None,
        formats=None,
        refresh_mode=resolved_refresh_mode,
    )
    prior_completed = store.latest_completed_run_for_run_key(run_key)

    replay_argv = [
        "run-all",
        "--base-dir",
        str(replay_base_dir),
        "--manifest",
        resolved_manifest_path,
        "--state-db",
        str(state_db),
        "--refresh-mode",
        resolved_refresh_mode,
    ]
    if source_run.config_dir:
        replay_argv.extend(["--config-dir", source_run.config_dir])

    replay_output = StringIO()
    try:
        with redirect_stdout(replay_output):
            exit_code = runner(replay_argv)
        if exit_code != 0:
            raise RuntimeError(f"Replay returned non-zero exit code: {exit_code}")
    except Exception as exc:
        rendered_command = " ".join(replay_argv)
        replay_logs = replay_output.getvalue().strip()
        failure_detail = f"Replay failed for run_id={source_run.run_id} via `{rendered_command}`: {exc}"
        if replay_logs:
            failure_detail = f"{failure_detail}. Captured pipeline output: {replay_logs}"
        raise RuntimeError(failure_detail) from exc

    result_run = store.latest_completed_run_for_run_key(run_key)
    if result_run is None:
        raise RuntimeError(f"Replay completed but no persisted run was found for run_key={run_key}")

    action = (
        "reused_completed_run"
        if prior_completed is not None and prior_completed.run_id == result_run.run_id
        else "replayed"
    )
    return ReplayRunOperationResult(
        action=action,
        requested_run=source_run,
        result_run=result_run,
        state_db=state_db.resolve(),
        base_dir=replay_base_dir.resolve(),
        refresh_mode=resolved_refresh_mode,
        replay_command=tuple(replay_argv),
    )
