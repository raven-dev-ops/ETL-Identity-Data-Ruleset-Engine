"""Shared operator workflow actions used by CLI and service surfaces."""

from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Callable, Sequence

from etl_identity_engine.delivery_publish import publish_delivery_snapshot
from etl_identity_engine.ingest.manifest import peek_manifest_batch_id
from etl_identity_engine.ingest.replay_bundle import (
    replay_bundle_is_replayable_from_summary,
    replay_bundle_replay_manifest_path_from_summary,
)
from etl_identity_engine.output_contracts import DELIVERY_CONTRACT_NAME, DELIVERY_CONTRACT_VERSION
from etl_identity_engine.runtime_config import ExportJobConfig
from etl_identity_engine.storage.sqlite_store import (
    ExportJobRunRecord,
    PersistedReviewCase,
    PipelineRunRecord,
    PipelineStateStore,
    build_export_key,
    build_run_key,
    normalize_tenant_id,
)
from etl_identity_engine.storage.state_store_target import state_store_display_name


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
    state_db: str
    base_dir: Path
    refresh_mode: str
    replay_command: tuple[str, ...]


@dataclass(frozen=True)
class PublishRunOperationResult:
    action: str
    run: PipelineRunRecord
    contract_version: str
    snapshot_dir: Path
    current_pointer_path: Path


@dataclass(frozen=True)
class ExportJobRunOperationResult:
    action: str
    job: ExportJobConfig
    source_run: PipelineRunRecord
    export_run: ExportJobRunRecord


def _resolve_replay_manifest_path(source_run: PipelineRunRecord) -> Path:
    replay_manifest_path = replay_bundle_replay_manifest_path_from_summary(source_run.summary)
    if replay_manifest_path is not None and replay_bundle_is_replayable_from_summary(source_run.summary):
        resolved_replay_manifest_path = replay_manifest_path.resolve()
        if resolved_replay_manifest_path.exists():
            return resolved_replay_manifest_path

    if source_run.manifest_path:
        manifest_path = Path(source_run.manifest_path).resolve()
        if manifest_path.exists():
            return manifest_path

    if replay_manifest_path is not None:
        resolved_replay_manifest_path = replay_manifest_path.resolve()
        if resolved_replay_manifest_path.exists():
            return resolved_replay_manifest_path
        raise FileNotFoundError(
            f"Cannot replay run_id={source_run.run_id} because neither the original manifest nor the "
            f"archived replay manifest exists: {resolved_replay_manifest_path}"
        )

    if not source_run.manifest_path:
        raise ValueError(f"Persisted run {source_run.run_id} is missing manifest_path and cannot be replayed")

    manifest_path = Path(source_run.manifest_path).resolve()
    raise FileNotFoundError(
        f"Cannot replay run_id={source_run.run_id} because manifest_path no longer exists "
        f"and no replayable archived bundle is recorded: {manifest_path}"
    )


def resolve_completed_run_id(
    store: PipelineStateStore,
    requested_run_id: str | None,
    *,
    tenant_id: str | None = None,
) -> str:
    run_id = requested_run_id or store.latest_completed_run_id(tenant_id=tenant_id)
    if run_id is None:
        raise FileNotFoundError(f"No completed persisted runs found in {store.display_name}")
    return run_id


def resolve_review_case_run_id(
    store: PipelineStateStore,
    requested_run_id: str | None,
    *,
    tenant_id: str | None = None,
) -> str:
    run_id = requested_run_id or store.latest_completed_run_id_with_review_cases(tenant_id=tenant_id)
    if run_id is None:
        raise FileNotFoundError(f"No completed persisted review-case runs found in {store.display_name}")
    return run_id


def apply_review_decision_operation(
    *,
    store: PipelineStateStore,
    tenant_id: str | None = None,
    run_id: str | None,
    review_id: str,
    decision: str,
    assigned_to: str | None,
    notes: str | None,
) -> ReviewDecisionOperationResult:
    resolved_tenant_id = normalize_tenant_id(tenant_id)
    resolved_run_id = resolve_review_case_run_id(store, run_id, tenant_id=resolved_tenant_id)
    existing = store.load_review_case(
        run_id=resolved_run_id,
        review_id=review_id,
        tenant_id=resolved_tenant_id,
    )

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
        tenant_id=resolved_tenant_id,
        queue_status=decision,
        assigned_to=assigned_to,
        operator_notes=notes,
    )
    return ReviewDecisionOperationResult(action="updated", case=updated)


def replay_run_operation(
    *,
    store: PipelineStateStore,
    tenant_id: str | None = None,
    state_db: str | Path,
    source_run_id: str | None,
    base_dir: Path | None,
    refresh_mode: str | None,
    runner: CommandRunner,
) -> ReplayRunOperationResult:
    resolved_tenant_id = normalize_tenant_id(tenant_id)
    resolved_source_run_id = resolve_completed_run_id(
        store,
        source_run_id,
        tenant_id=resolved_tenant_id,
    )
    source_run = store.load_run_record(resolved_source_run_id)

    if source_run.input_mode != "manifest":
        raise ValueError(
            f"replay-run currently supports persisted manifest runs only; "
            f"run_id={source_run.run_id} input_mode={source_run.input_mode!r}"
        )
    manifest_path = _resolve_replay_manifest_path(source_run)

    resolved_refresh_mode = refresh_mode or str(
        source_run.summary.get("run_context", {}).get("refresh_mode", "full")
    )
    replay_base_dir = base_dir if base_dir is not None else Path(source_run.base_dir)
    resolved_manifest_path = str(manifest_path)
    run_key = build_run_key(
        tenant_id=resolved_tenant_id,
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
    prior_completed = store.latest_completed_run_for_run_key(run_key, tenant_id=resolved_tenant_id)

    replay_argv = [
        "run-all",
        "--tenant-id",
        resolved_tenant_id,
        "--base-dir",
        str(replay_base_dir),
        "--manifest",
        resolved_manifest_path,
        "--state-db",
        str(state_db),
        "--refresh-mode",
        resolved_refresh_mode,
    ]
    replay_argv.extend(["--replay-source-run-id", source_run.run_id])
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

    result_run = store.latest_completed_run_for_run_key(run_key, tenant_id=resolved_tenant_id)
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
        state_db=state_store_display_name(state_db),
        base_dir=replay_base_dir.resolve(),
        refresh_mode=resolved_refresh_mode,
        replay_command=tuple(replay_argv),
    )


def publish_run_operation(
    *,
    store: PipelineStateStore,
    tenant_id: str | None = None,
    state_db: str | Path,
    run_id: str | None,
    output_dir: Path,
    contract_version: str = DELIVERY_CONTRACT_VERSION,
) -> PublishRunOperationResult:
    resolved_tenant_id = normalize_tenant_id(tenant_id)
    resolved_run_id = resolve_completed_run_id(store, run_id, tenant_id=resolved_tenant_id)
    run = store.load_run_record(resolved_run_id)
    contract_root = output_dir / DELIVERY_CONTRACT_NAME / contract_version
    snapshot_dir = contract_root / "snapshots" / resolved_run_id
    snapshot_existed = snapshot_dir.exists()
    bundle = store.load_run_bundle(resolved_run_id)
    published = publish_delivery_snapshot(
        bundle=bundle,
        state_db_path=state_db,
        output_root=output_dir,
        contract_version=contract_version,
    )
    return PublishRunOperationResult(
        action="reused_snapshot" if snapshot_existed else "published",
        run=run,
        contract_version=contract_version,
        snapshot_dir=published.snapshot_dir,
        current_pointer_path=published.current_pointer_path,
    )


def export_job_run_operation(
    *,
    store: PipelineStateStore,
    tenant_id: str | None = None,
    state_db: str | Path,
    source_run_id: str | None,
    job: ExportJobConfig,
) -> ExportJobRunOperationResult:
    resolved_tenant_id = normalize_tenant_id(tenant_id)
    resolved_source_run_id = resolve_completed_run_id(
        store,
        source_run_id,
        tenant_id=resolved_tenant_id,
    )
    source_run = store.load_run_record(resolved_source_run_id)
    if source_run.status != "completed":
        raise ValueError(
            f"Only completed persisted runs can be exported, received status={source_run.status!r}"
        )

    export_key = build_export_key(
        tenant_id=resolved_tenant_id,
        job_name=job.name,
        source_run_id=resolved_source_run_id,
        contract_name=job.contract_name,
        contract_version=job.contract_version,
        output_root=str(job.output_root),
    )
    started_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    start_decision = store.begin_export_run(
        tenant_id=resolved_tenant_id,
        export_key=export_key,
        job_name=job.name,
        source_run_id=resolved_source_run_id,
        contract_name=job.contract_name,
        contract_version=job.contract_version,
        output_root=str(job.output_root),
        started_at_utc=started_at_utc,
    )

    export_run_id = start_decision.export_run_id
    try:
        bundle = store.load_run_bundle(resolved_source_run_id)
        published = publish_delivery_snapshot(
            bundle=bundle,
            state_db_path=state_db,
            output_root=job.output_root,
            contract_version=job.contract_version,
        )
        if start_decision.action == "reuse_completed":
            export_record = store.load_export_run_record(export_run_id)
            action = "reused_completed_export"
        else:
            finished_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            row_counts = {
                "golden_records": len(bundle.golden_rows),
                "source_to_golden_crosswalk": len(bundle.crosswalk_rows),
            }
            metadata = {
                "tenant_id": resolved_tenant_id,
                "job": {
                    "name": job.name,
                    "consumer": job.consumer,
                    "description": job.description,
                    "output_root": str(job.output_root),
                    "contract_name": job.contract_name,
                    "contract_version": job.contract_version,
                    "format": job.export_format,
                },
                "source_run": {
                    "run_id": source_run.run_id,
                    "tenant_id": source_run.tenant_id,
                    "run_key": source_run.run_key,
                    "attempt_number": source_run.attempt_number,
                    "batch_id": source_run.batch_id,
                    "input_mode": source_run.input_mode,
                    "manifest_path": source_run.manifest_path,
                    "base_dir": source_run.base_dir,
                    "config_dir": source_run.config_dir,
                    "profile": source_run.profile,
                    "seed": source_run.seed,
                    "formats": source_run.formats,
                    "status": source_run.status,
                    "started_at_utc": source_run.started_at_utc,
                    "finished_at_utc": source_run.finished_at_utc,
                    "total_records": source_run.total_records,
                    "candidate_pair_count": source_run.candidate_pair_count,
                    "cluster_count": source_run.cluster_count,
                    "golden_record_count": source_run.golden_record_count,
                    "review_queue_count": source_run.review_queue_count,
                    "failure_detail": source_run.failure_detail,
                    "resumed_from_run_id": source_run.resumed_from_run_id,
                    "summary": source_run.summary,
                },
            }
            store.complete_export_run(
                export_run_id=export_run_id,
                finished_at_utc=finished_at_utc,
                snapshot_dir=str(published.snapshot_dir),
                current_pointer_path=str(published.current_pointer_path),
                row_counts=row_counts,
                metadata=metadata,
            )
            export_record = store.load_export_run_record(export_run_id)
            action = "exported"
    except Exception as exc:
        if start_decision.action == "start_new":
            store.mark_export_run_failed(
                export_run_id=export_run_id,
                finished_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                failure_detail=str(exc),
            )
        raise

    return ExportJobRunOperationResult(
        action=action,
        job=job,
        source_run=source_run,
        export_run=export_record,
    )
