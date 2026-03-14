"""SQL-backed persistence for pipeline run state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, RowMapping

from etl_identity_engine.review_cases import (
    REVIEW_CASE_STATUSES,
    validate_review_case_status,
    validate_review_case_transition,
)
from etl_identity_engine.output_contracts import (
    BLOCKING_METRICS_HEADERS,
    CROSSWALK_HEADERS,
    ENTITY_CLUSTER_HEADERS,
    GOLDEN_HEADERS,
    MANUAL_REVIEW_HEADERS,
    MATCH_SCORE_HEADERS,
    NORMALIZED_HEADERS,
)
from etl_identity_engine.observability import sanitize_observability_fields
from etl_identity_engine.storage.migration_runner import upgrade_state_store
from etl_identity_engine.storage.state_store_target import resolve_state_store_target, create_state_store_engine


RUN_STATUSES = frozenset({"running", "completed", "failed"})
EXPORT_RUN_STATUSES = frozenset({"running", "completed", "failed"})
AUDIT_EVENT_STATUSES = frozenset({"succeeded", "failed", "noop", "reused"})
RUN_CHECKPOINT_STAGES = (
    "normalize",
    "match",
    "cluster",
    "review_queue",
    "golden",
    "crosswalk",
    "report",
)
RUN_CHECKPOINT_STAGE_ORDER = {
    stage_name: index
    for index, stage_name in enumerate(RUN_CHECKPOINT_STAGES, start=1)
}
ARTIFACT_TABLE_NAMES = (
    "normalized_source_records",
    "candidate_pairs",
    "blocking_metrics",
    "entity_clusters",
    "golden_records",
    "source_to_golden_crosswalk",
    "review_cases",
)


@dataclass(frozen=True)
class PipelineRunRecord:
    run_id: str
    run_key: str
    attempt_number: int
    batch_id: str | None
    input_mode: str
    manifest_path: str | None
    base_dir: str
    config_dir: str | None
    profile: str | None
    seed: int | None
    formats: str | None
    status: str
    started_at_utc: str
    finished_at_utc: str
    total_records: int
    candidate_pair_count: int
    cluster_count: int
    golden_record_count: int
    review_queue_count: int
    failure_detail: str | None
    resumed_from_run_id: str | None
    summary: dict[str, object]


@dataclass(frozen=True)
class PersistedRunBundle:
    run: PipelineRunRecord
    normalized_rows: list[dict[str, str]]
    candidate_pairs: list[dict[str, str]]
    blocking_metrics_rows: list[dict[str, str]]
    cluster_rows: list[dict[str, str]]
    golden_rows: list[dict[str, str]]
    crosswalk_rows: list[dict[str, str]]
    review_rows: list[dict[str, str]]


@dataclass(frozen=True)
class PersistRunMetadata:
    run_id: str
    run_key: str
    attempt_number: int
    batch_id: str | None
    input_mode: str
    manifest_path: str | None
    base_dir: str
    config_dir: str | None
    profile: str | None
    seed: int | None
    formats: str | None
    started_at_utc: str
    finished_at_utc: str
    status: str
    failure_detail: str | None = None


@dataclass(frozen=True)
class RunStartDecision:
    action: str
    run_id: str
    run_key: str
    attempt_number: int
    started_at_utc: str
    resume_from_run_id: str | None = None
    resume_checkpoint_stage: str | None = None
    resume_checkpointed_at_utc: str | None = None


@dataclass(frozen=True)
class RunCheckpointRecord:
    checkpoint_id: str
    run_id: str
    run_key: str
    attempt_number: int
    stage_name: str
    stage_order: int
    checkpointed_at_utc: str
    total_duration_seconds: float
    record_counts: dict[str, int]
    phase_metrics: dict[str, dict[str, float | int]]
    normalized_rows: list[dict[str, str]]
    match_rows: list[dict[str, str]]
    blocking_metrics_rows: list[dict[str, str]]
    cluster_rows: list[dict[str, str]]
    golden_rows: list[dict[str, str]]
    crosswalk_rows: list[dict[str, str]]
    review_rows: list[dict[str, str]]
    active_review_rows: list[dict[str, str]]
    summary: dict[str, object]


@dataclass(frozen=True)
class ExportJobRunRecord:
    export_run_id: str
    export_key: str
    attempt_number: int
    job_name: str
    source_run_id: str
    contract_name: str
    contract_version: str
    output_root: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    snapshot_dir: str
    current_pointer_path: str
    row_counts: dict[str, int]
    metadata: dict[str, object]
    failure_detail: str | None


@dataclass(frozen=True)
class ExportStartDecision:
    action: str
    export_run_id: str
    export_key: str
    attempt_number: int
    started_at_utc: str


@dataclass(frozen=True)
class AuditEventRecord:
    audit_event_id: str
    occurred_at_utc: str
    actor_type: str
    actor_id: str
    action: str
    resource_type: str
    resource_id: str
    run_id: str | None
    status: str
    details: dict[str, object]


@dataclass(frozen=True)
class StoreOperationalMetrics:
    run_status_counts: dict[str, int]
    export_status_counts: dict[str, int]
    review_case_status_counts: dict[str, int]
    audit_event_count: int
    latest_completed_run_id: str | None
    latest_completed_run_finished_at_utc: str | None
    latest_failed_run_id: str | None
    latest_failed_run_finished_at_utc: str | None


@dataclass(frozen=True)
class PersistedReviewCase:
    run_id: str
    review_id: str
    left_id: str
    right_id: str
    score: float
    reason_codes: str
    top_contributing_match_signals: str
    queue_status: str
    assigned_to: str
    operator_notes: str
    created_at_utc: str
    updated_at_utc: str
    resolved_at_utc: str


@dataclass(frozen=True)
class PaginatedResult:
    items: list[object]
    total_count: int
    next_page_token: str | None


ARTIFACT_SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "normalized_source_records": tuple((column, "TEXT") for column in NORMALIZED_HEADERS),
    "candidate_pairs": (
        ("left_id", "TEXT"),
        ("right_id", "TEXT"),
        ("score", "REAL"),
        ("decision", "TEXT"),
        ("matched_fields", "TEXT"),
        ("reason_trace", "TEXT"),
    ),
    "blocking_metrics": (
        ("pass_name", "TEXT"),
        ("fields", "TEXT"),
        ("raw_candidate_pair_count", "INTEGER"),
        ("new_candidate_pair_count", "INTEGER"),
        ("cumulative_candidate_pair_count", "INTEGER"),
        ("overall_deduplicated_candidate_pair_count", "INTEGER"),
    ),
    "entity_clusters": tuple((column, "TEXT") for column in ENTITY_CLUSTER_HEADERS),
    "golden_records": tuple((column, "TEXT") for column in GOLDEN_HEADERS),
    "source_to_golden_crosswalk": tuple((column, "TEXT") for column in CROSSWALK_HEADERS),
    "review_cases": (
        ("review_id", "TEXT"),
        ("left_id", "TEXT"),
        ("right_id", "TEXT"),
        ("score", "REAL"),
        ("reason_codes", "TEXT"),
        ("top_contributing_match_signals", "TEXT"),
        ("queue_status", "TEXT"),
        ("assigned_to", "TEXT"),
        ("operator_notes", "TEXT"),
        ("created_at_utc", "TEXT"),
        ("updated_at_utc", "TEXT"),
        ("resolved_at_utc", "TEXT"),
    ),
}

ARTIFACT_HEADERS: dict[str, tuple[str, ...]] = {
    "normalized_source_records": NORMALIZED_HEADERS,
    "candidate_pairs": MATCH_SCORE_HEADERS,
    "blocking_metrics": BLOCKING_METRICS_HEADERS,
    "entity_clusters": ENTITY_CLUSTER_HEADERS,
    "golden_records": GOLDEN_HEADERS,
    "source_to_golden_crosswalk": CROSSWALK_HEADERS,
    "review_cases": MANUAL_REVIEW_HEADERS,
}

ARTIFACT_INDEXES: dict[str, tuple[tuple[str, ...], ...]] = {
    "normalized_source_records": (("source_record_id",), ("person_entity_id",), ("source_system",)),
    "candidate_pairs": (("left_id",), ("right_id",), ("decision",)),
    "blocking_metrics": (("pass_name",),),
    "entity_clusters": (("cluster_id",), ("source_record_id",), ("person_entity_id",)),
    "golden_records": (("golden_id",), ("cluster_id",), ("person_entity_id",)),
    "source_to_golden_crosswalk": (("source_record_id",), ("golden_id",), ("cluster_id",)),
    "review_cases": (("review_id",), ("queue_status",), ("assigned_to",), ("left_id",), ("right_id",)),
}

PIPELINE_STATE_TABLES = ("pipeline_runs", "run_checkpoints", "export_job_runs", "audit_events", *ARTIFACT_TABLE_NAMES)

RUN_LIST_SORTS = {
    "finished_at_desc": "finished_at_utc DESC, started_at_utc DESC, run_id DESC",
    "finished_at_asc": "finished_at_utc ASC, started_at_utc ASC, run_id ASC",
    "started_at_desc": "started_at_utc DESC, run_id DESC",
    "started_at_asc": "started_at_utc ASC, run_id ASC",
}
GOLDEN_LIST_SORTS = {
    "golden_id_asc": "golden_id ASC",
    "golden_id_desc": "golden_id DESC",
    "last_name_asc": "last_name ASC, first_name ASC, golden_id ASC",
    "last_name_desc": "last_name DESC, first_name DESC, golden_id DESC",
}
REVIEW_CASE_LIST_SORTS = {
    "queue_order_asc": "row_index ASC, review_id ASC",
    "queue_order_desc": "row_index DESC, review_id DESC",
    "score_desc": "score DESC, review_id ASC",
    "score_asc": "score ASC, review_id ASC",
    "updated_at_desc": "updated_at_utc DESC, review_id DESC",
    "updated_at_asc": "updated_at_utc ASC, review_id ASC",
}


def build_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"RUN-{timestamp}-{uuid4().hex[:8].upper()}"


def build_export_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"EXP-{timestamp}-{uuid4().hex[:8].upper()}"


def build_audit_event_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"AUD-{timestamp}-{uuid4().hex[:8].upper()}"


def build_checkpoint_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"CHK-{timestamp}-{uuid4().hex[:8].upper()}"


def build_run_key(
    *,
    input_mode: str,
    manifest_path: str | None,
    batch_id: str | None,
    config_dir: str | None,
    profile: str | None,
    seed: int | None,
    person_count: int | None,
    duplicate_rate: float | None,
    formats: str | None,
    refresh_mode: str | None,
    source_run_id: str | None = None,
    stream_id: str | None = None,
    stream_first_sequence: int | None = None,
    stream_last_sequence: int | None = None,
    stream_event_hash: str | None = None,
) -> str:
    payload = json.dumps(
        {
            "input_mode": input_mode,
            "manifest_path": manifest_path,
            "batch_id": batch_id,
            "config_dir": config_dir,
            "profile": profile,
            "seed": seed,
            "person_count": person_count,
            "duplicate_rate": duplicate_rate,
            "formats": formats,
            "refresh_mode": refresh_mode,
            "source_run_id": source_run_id,
            "stream_id": stream_id,
            "stream_first_sequence": stream_first_sequence,
            "stream_last_sequence": stream_last_sequence,
            "stream_event_hash": stream_event_hash,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()
    return f"RK-{digest}"


def build_export_key(
    *,
    job_name: str,
    source_run_id: str,
    contract_name: str,
    contract_version: str,
    output_root: str,
) -> str:
    payload = json.dumps(
        {
            "job_name": job_name,
            "source_run_id": source_run_id,
            "contract_name": contract_name,
            "contract_version": contract_version,
            "output_root": output_root,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()
    return f"EK-{digest}"


def bootstrap_state_store(state_db: str | Path) -> None:
    upgrade_state_store(state_db)


def bootstrap_sqlite_store(db_path: Path) -> None:
    upgrade_state_store(Path(db_path))


def _row_to_strings(row: Mapping[str, object], headers: tuple[str, ...]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for header in headers:
        value = row[header]
        resolved[header] = "" if value is None else str(value)
    return resolved


def _row_to_review_case(row: Mapping[str, object]) -> PersistedReviewCase:
    score = row["score"]
    return PersistedReviewCase(
        run_id=str(row["run_id"]),
        review_id=str(row["review_id"]),
        left_id=str(row["left_id"]),
        right_id=str(row["right_id"]),
        score=float(score or 0.0),
        reason_codes="" if row["reason_codes"] is None else str(row["reason_codes"]),
        top_contributing_match_signals=""
        if row["top_contributing_match_signals"] is None
        else str(row["top_contributing_match_signals"]),
        queue_status="" if row["queue_status"] is None else str(row["queue_status"]),
        assigned_to="" if row["assigned_to"] is None else str(row["assigned_to"]),
        operator_notes="" if row["operator_notes"] is None else str(row["operator_notes"]),
        created_at_utc="" if row["created_at_utc"] is None else str(row["created_at_utc"]),
        updated_at_utc="" if row["updated_at_utc"] is None else str(row["updated_at_utc"]),
        resolved_at_utc="" if row["resolved_at_utc"] is None else str(row["resolved_at_utc"]),
    )


def _row_to_audit_event(row: Mapping[str, object]) -> AuditEventRecord:
    return AuditEventRecord(
        audit_event_id=str(row["audit_event_id"]),
        occurred_at_utc=str(row["occurred_at_utc"]),
        actor_type=str(row["actor_type"]),
        actor_id=str(row["actor_id"]),
        action=str(row["action"]),
        resource_type=str(row["resource_type"]),
        resource_id=str(row["resource_id"]),
        run_id=None if row["run_id"] in (None, "") else str(row["run_id"]),
        status=str(row["status"]),
        details=json.loads(str(row["details_json"] or "{}")),
    )


def _row_to_run_checkpoint(row: Mapping[str, object]) -> RunCheckpointRecord:
    payload = json.loads(str(row["payload_json"] or "{}"))
    return RunCheckpointRecord(
        checkpoint_id=str(row["checkpoint_id"]),
        run_id=str(row["run_id"]),
        run_key=str(row["run_key"]),
        attempt_number=int(row["attempt_number"] or 0),
        stage_name=str(row["stage_name"]),
        stage_order=int(row["stage_order"] or 0),
        checkpointed_at_utc=str(row["checkpointed_at_utc"]),
        total_duration_seconds=float(row["total_duration_seconds"] or 0.0),
        record_counts=json.loads(str(row["record_counts_json"] or "{}")),
        phase_metrics=json.loads(str(row["phase_metrics_json"] or "{}")),
        normalized_rows=payload.get("normalized_rows", []),
        match_rows=payload.get("match_rows", []),
        blocking_metrics_rows=payload.get("blocking_metrics_rows", []),
        cluster_rows=payload.get("cluster_rows", []),
        golden_rows=payload.get("golden_rows", []),
        crosswalk_rows=payload.get("crosswalk_rows", []),
        review_rows=payload.get("review_rows", []),
        active_review_rows=payload.get("active_review_rows", []),
        summary=payload.get("summary", {}),
    )


def _row_to_run_record(row: Mapping[str, object]) -> PipelineRunRecord:
    return PipelineRunRecord(
        run_id=str(row["run_id"]),
        run_key="" if row["run_key"] is None else str(row["run_key"]),
        attempt_number=int(row["attempt_number"] or 0),
        batch_id=None if row["batch_id"] is None else str(row["batch_id"]),
        input_mode=str(row["input_mode"]),
        manifest_path=None if row["manifest_path"] is None else str(row["manifest_path"]),
        base_dir=str(row["base_dir"]),
        config_dir=None if row["config_dir"] is None else str(row["config_dir"]),
        profile=None if row["profile"] is None else str(row["profile"]),
        seed=None if row["seed"] is None else int(row["seed"]),
        formats=None if row["formats"] is None else str(row["formats"]),
        status=str(row["status"]),
        started_at_utc=str(row["started_at_utc"]),
        finished_at_utc=str(row["finished_at_utc"]),
        total_records=int(row["total_records"]),
        candidate_pair_count=int(row["candidate_pair_count"]),
        cluster_count=int(row["cluster_count"]),
        golden_record_count=int(row["golden_record_count"]),
        review_queue_count=int(row["review_queue_count"]),
        failure_detail=None if row["failure_detail"] in (None, "") else str(row["failure_detail"]),
        resumed_from_run_id=None
        if row["resumed_from_run_id"] in (None, "")
        else str(row["resumed_from_run_id"]),
        summary=json.loads(str(row["summary_json"] or "{}")),
    )


def _build_next_page_token(*, offset: int, returned_count: int, total_count: int) -> str | None:
    next_offset = offset + returned_count
    if next_offset >= total_count:
        return None
    return str(next_offset)


def _validate_pagination(*, limit: int, offset: int) -> None:
    if limit <= 0:
        raise ValueError("Pagination limit must be greater than 0")
    if offset < 0:
        raise ValueError("Pagination offset must be greater than or equal to 0")


def _normalize_search_query(query: str | None) -> str | None:
    if query is None:
        return None
    normalized = query.strip().lower()
    return normalized or None


class SQLitePipelineStore:
    def __init__(self, db_path: str | Path):
        self.target = resolve_state_store_target(db_path)
        self.state_db = self.target.raw_value
        self.display_name = self.target.display_name
        self.db_path = self.target.file_path
        bootstrap_state_store(self.state_db)
        self.engine: Engine = create_state_store_engine(self.target)

    def _fetchone(
        self,
        connection: Connection,
        sql: str,
        parameters: dict[str, object] | None = None,
    ) -> RowMapping | None:
        return connection.execute(text(sql), parameters or {}).mappings().one_or_none()

    def _fetchall(
        self,
        connection: Connection,
        sql: str,
        parameters: dict[str, object] | None = None,
    ) -> list[RowMapping]:
        return list(connection.execute(text(sql), parameters or {}).mappings().all())

    def _execute_many(
        self,
        connection: Connection,
        sql: str,
        parameter_sets: list[dict[str, object]],
    ) -> None:
        if not parameter_sets:
            return
        connection.execute(text(sql), parameter_sets)

    def _clear_existing_run(self, connection: Connection, run_id: str) -> None:
        for table_name in ARTIFACT_TABLE_NAMES:
            connection.execute(text(f"DELETE FROM {table_name} WHERE run_id = :run_id"), {"run_id": run_id})

    def begin_run(
        self,
        *,
        run_key: str,
        batch_id: str | None,
        input_mode: str,
        manifest_path: str | None,
        base_dir: str,
        config_dir: str | None,
        profile: str | None,
        seed: int | None,
        formats: str | None,
        started_at_utc: str,
    ) -> RunStartDecision:
        with self.engine.begin() as connection:
            completed = self._fetchone(
                connection,
                """
                SELECT run_id, attempt_number
                FROM pipeline_runs
                WHERE run_key = :run_key AND status = 'completed'
                ORDER BY attempt_number DESC
                LIMIT 1
                """,
                {"run_key": run_key},
            )
            if completed is not None:
                return RunStartDecision(
                    action="reuse_completed",
                    run_id=str(completed["run_id"]),
                    run_key=run_key,
                    attempt_number=int(completed["attempt_number"] or 1),
                    started_at_utc=started_at_utc,
                )

            resumable_checkpoint = self._fetchone(
                connection,
                """
                SELECT run_checkpoints.run_id,
                       run_checkpoints.stage_name,
                       run_checkpoints.checkpointed_at_utc
                FROM run_checkpoints
                JOIN pipeline_runs
                  ON pipeline_runs.run_id = run_checkpoints.run_id
                WHERE pipeline_runs.run_key = :run_key
                  AND pipeline_runs.status = 'failed'
                ORDER BY pipeline_runs.attempt_number DESC,
                         run_checkpoints.stage_order DESC,
                         run_checkpoints.checkpointed_at_utc DESC,
                         run_checkpoints.checkpoint_id DESC
                LIMIT 1
                """,
                {"run_key": run_key},
            )

            attempt_row = self._fetchone(
                connection,
                "SELECT COALESCE(MAX(attempt_number), 0) AS max_attempt FROM pipeline_runs WHERE run_key = :run_key",
                {"run_key": run_key},
            )
            attempt_number = int(attempt_row["max_attempt"] or 0) + 1
            run_id = build_run_id(datetime.fromisoformat(started_at_utc.replace("Z", "+00:00")))
            resumed_from_run_id = (
                None if resumable_checkpoint is None else str(resumable_checkpoint["run_id"])
            )
            connection.execute(
                text(
                    """
                    INSERT INTO pipeline_runs (
                        run_id,
                        run_key,
                        attempt_number,
                        batch_id,
                        input_mode,
                        manifest_path,
                        base_dir,
                        config_dir,
                        profile,
                        seed,
                        formats,
                        resumed_from_run_id,
                        status,
                        started_at_utc,
                        finished_at_utc,
                        total_records,
                        candidate_pair_count,
                        cluster_count,
                        golden_record_count,
                        review_queue_count,
                        failure_detail,
                        summary_json
                    ) VALUES (
                        :run_id,
                        :run_key,
                        :attempt_number,
                        :batch_id,
                        :input_mode,
                        :manifest_path,
                        :base_dir,
                        :config_dir,
                        :profile,
                        :seed,
                        :formats,
                        :resumed_from_run_id,
                        'running',
                        :started_at_utc,
                        '',
                        0,
                        0,
                        0,
                        0,
                        0,
                        '',
                        '{}'
                    )
                    """
                ),
                {
                    "run_id": run_id,
                    "run_key": run_key,
                    "attempt_number": attempt_number,
                    "batch_id": batch_id,
                    "input_mode": input_mode,
                    "manifest_path": manifest_path,
                    "base_dir": base_dir,
                    "config_dir": config_dir,
                    "profile": profile,
                    "seed": seed,
                    "formats": formats,
                    "resumed_from_run_id": resumed_from_run_id,
                    "started_at_utc": started_at_utc,
                },
            )

        return RunStartDecision(
            action="resume_failed" if resumable_checkpoint is not None else "start_new",
            run_id=run_id,
            run_key=run_key,
            attempt_number=attempt_number,
            started_at_utc=started_at_utc,
            resume_from_run_id=None if resumable_checkpoint is None else str(resumable_checkpoint["run_id"]),
            resume_checkpoint_stage=None
            if resumable_checkpoint is None
            else str(resumable_checkpoint["stage_name"]),
            resume_checkpointed_at_utc=None
            if resumable_checkpoint is None
            else str(resumable_checkpoint["checkpointed_at_utc"]),
        )

    def begin_export_run(
        self,
        *,
        export_key: str,
        job_name: str,
        source_run_id: str,
        contract_name: str,
        contract_version: str,
        output_root: str,
        started_at_utc: str,
    ) -> ExportStartDecision:
        with self.engine.begin() as connection:
            completed = self._fetchone(
                connection,
                """
                SELECT export_run_id, attempt_number
                FROM export_job_runs
                WHERE export_key = :export_key AND status = 'completed'
                ORDER BY attempt_number DESC
                LIMIT 1
                """,
                {"export_key": export_key},
            )
            if completed is not None:
                return ExportStartDecision(
                    action="reuse_completed",
                    export_run_id=str(completed["export_run_id"]),
                    export_key=export_key,
                    attempt_number=int(completed["attempt_number"] or 1),
                    started_at_utc=started_at_utc,
                )

            attempt_row = self._fetchone(
                connection,
                """
                SELECT COALESCE(MAX(attempt_number), 0) AS max_attempt
                FROM export_job_runs
                WHERE export_key = :export_key
                """,
                {"export_key": export_key},
            )
            attempt_number = int(attempt_row["max_attempt"] or 0) + 1
            export_run_id = build_export_run_id(
                datetime.fromisoformat(started_at_utc.replace("Z", "+00:00"))
            )
            connection.execute(
                text(
                    """
                    INSERT INTO export_job_runs (
                        export_run_id,
                        export_key,
                        attempt_number,
                        job_name,
                        source_run_id,
                        contract_name,
                        contract_version,
                        output_root,
                        status,
                        started_at_utc,
                        finished_at_utc,
                        snapshot_dir,
                        current_pointer_path,
                        row_counts_json,
                        metadata_json,
                        failure_detail
                    ) VALUES (
                        :export_run_id,
                        :export_key,
                        :attempt_number,
                        :job_name,
                        :source_run_id,
                        :contract_name,
                        :contract_version,
                        :output_root,
                        'running',
                        :started_at_utc,
                        '',
                        '',
                        '',
                        '{}',
                        '{}',
                        ''
                    )
                    """
                ),
                {
                    "export_run_id": export_run_id,
                    "export_key": export_key,
                    "attempt_number": attempt_number,
                    "job_name": job_name,
                    "source_run_id": source_run_id,
                    "contract_name": contract_name,
                    "contract_version": contract_version,
                    "output_root": output_root,
                    "started_at_utc": started_at_utc,
                },
            )

        return ExportStartDecision(
            action="start_new",
            export_run_id=export_run_id,
            export_key=export_key,
            attempt_number=attempt_number,
            started_at_utc=started_at_utc,
        )

    def _persist_artifact_rows(
        self,
        connection: Connection,
        table_name: str,
        run_id: str,
        rows: list[dict[str, object]],
    ) -> None:
        headers = ARTIFACT_HEADERS[table_name]
        quoted_columns = ", ".join(f'"{column}"' for column in ("run_id", "row_index", *headers))
        placeholders = ", ".join(f":{column}" for column in ("run_id", "row_index", *headers))
        self._execute_many(
            connection,
            f"INSERT INTO {table_name} ({quoted_columns}) VALUES ({placeholders})",
            [
                {
                    "run_id": run_id,
                    "row_index": index,
                    **{header: row.get(header, "") for header in headers},
                }
                for index, row in enumerate(rows)
            ],
        )

    def _persist_review_case_rows(
        self,
        connection: Connection,
        run_id: str,
        rows: list[dict[str, str | float]],
        *,
        created_at_utc: str,
    ) -> None:
        self._execute_many(
            connection,
            """
            INSERT INTO review_cases (
                run_id,
                row_index,
                review_id,
                left_id,
                right_id,
                score,
                reason_codes,
                top_contributing_match_signals,
                queue_status,
                assigned_to,
                operator_notes,
                created_at_utc,
                updated_at_utc,
                resolved_at_utc
            ) VALUES (
                :run_id,
                :row_index,
                :review_id,
                :left_id,
                :right_id,
                :score,
                :reason_codes,
                :top_contributing_match_signals,
                :queue_status,
                :assigned_to,
                :operator_notes,
                :created_at_utc,
                :updated_at_utc,
                :resolved_at_utc
            )
            """,
            [
                {
                    "run_id": run_id,
                    "row_index": index,
                    "review_id": row.get("review_id", ""),
                    "left_id": row.get("left_id", ""),
                    "right_id": row.get("right_id", ""),
                    "score": row.get("score", ""),
                    "reason_codes": row.get("reason_codes", ""),
                    "top_contributing_match_signals": row.get("top_contributing_match_signals", ""),
                    "queue_status": validate_review_case_status(str(row.get("queue_status", "pending") or "pending")),
                    "assigned_to": str(row.get("assigned_to", "") or ""),
                    "operator_notes": str(row.get("operator_notes", "") or ""),
                    "created_at_utc": str(row.get("created_at_utc", "") or created_at_utc),
                    "updated_at_utc": str(row.get("updated_at_utc", "") or created_at_utc),
                    "resolved_at_utc": str(row.get("resolved_at_utc", "") or ""),
                }
                for index, row in enumerate(rows)
            ],
        )

    def persist_run_checkpoint(
        self,
        *,
        run_id: str,
        run_key: str,
        attempt_number: int,
        stage_name: str,
        checkpointed_at_utc: str,
        total_duration_seconds: float,
        phase_metrics: dict[str, dict[str, float | int]],
        normalized_rows: list[dict[str, str]],
        match_rows: list[dict[str, str | float]],
        blocking_metrics_rows: list[dict[str, str | int]],
        cluster_rows: list[dict[str, str]],
        golden_rows: list[dict[str, str]],
        crosswalk_rows: list[dict[str, str]],
        review_rows: list[dict[str, str | float]],
        active_review_rows: list[dict[str, str | float]],
        summary: dict[str, object],
    ) -> RunCheckpointRecord:
        if stage_name not in RUN_CHECKPOINT_STAGE_ORDER:
            raise ValueError(
                f"Unsupported run checkpoint stage {stage_name!r}; expected one of {sorted(RUN_CHECKPOINT_STAGE_ORDER)}"
            )

        stage_order = RUN_CHECKPOINT_STAGE_ORDER[stage_name]
        checkpoint_id = build_checkpoint_id(
            datetime.fromisoformat(checkpointed_at_utc.replace("Z", "+00:00"))
        )
        payload = {
            "normalized_rows": normalized_rows,
            "match_rows": match_rows,
            "blocking_metrics_rows": blocking_metrics_rows,
            "cluster_rows": cluster_rows,
            "golden_rows": golden_rows,
            "crosswalk_rows": crosswalk_rows,
            "review_rows": review_rows,
            "active_review_rows": active_review_rows,
            "summary": summary,
        }
        record_counts = {
            "normalized_rows": len(normalized_rows),
            "match_rows": len(match_rows),
            "blocking_metrics_rows": len(blocking_metrics_rows),
            "cluster_rows": len(cluster_rows),
            "golden_rows": len(golden_rows),
            "crosswalk_rows": len(crosswalk_rows),
            "review_rows": len(review_rows),
            "active_review_rows": len(active_review_rows),
        }

        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM run_checkpoints
                    WHERE run_id = :run_id AND stage_order >= :stage_order
                    """
                ),
                {"run_id": run_id, "stage_order": stage_order},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO run_checkpoints (
                        checkpoint_id,
                        run_id,
                        run_key,
                        attempt_number,
                        stage_name,
                        stage_order,
                        checkpointed_at_utc,
                        total_duration_seconds,
                        record_counts_json,
                        phase_metrics_json,
                        payload_json
                    ) VALUES (
                        :checkpoint_id,
                        :run_id,
                        :run_key,
                        :attempt_number,
                        :stage_name,
                        :stage_order,
                        :checkpointed_at_utc,
                        :total_duration_seconds,
                        :record_counts_json,
                        :phase_metrics_json,
                        :payload_json
                    )
                    """
                ),
                {
                    "checkpoint_id": checkpoint_id,
                    "run_id": run_id,
                    "run_key": run_key,
                    "attempt_number": attempt_number,
                    "stage_name": stage_name,
                    "stage_order": stage_order,
                    "checkpointed_at_utc": checkpointed_at_utc,
                    "total_duration_seconds": round(total_duration_seconds, 6),
                    "record_counts_json": json.dumps(record_counts, sort_keys=True),
                    "phase_metrics_json": json.dumps(phase_metrics, sort_keys=True),
                    "payload_json": json.dumps(payload, sort_keys=True),
                },
            )

        return self.load_run_checkpoint(run_id=run_id, stage_name=stage_name)

    def persist_run(
        self,
        *,
        metadata: PersistRunMetadata,
        normalized_rows: list[dict[str, str]],
        match_rows: list[dict[str, str | float]],
        blocking_metrics_rows: list[dict[str, str | int]],
        cluster_rows: list[dict[str, str]],
        golden_rows: list[dict[str, str]],
        crosswalk_rows: list[dict[str, str]],
        review_rows: list[dict[str, str | float]],
        summary: dict[str, object],
    ) -> None:
        if metadata.status != "completed":
            raise ValueError(f"Unsupported persisted run status: {metadata.status}")

        with self.engine.begin() as connection:
            self._clear_existing_run(connection, metadata.run_id)
            connection.execute(
                text(
                    """
                    UPDATE pipeline_runs
                    SET run_key = :run_key,
                        attempt_number = :attempt_number,
                        batch_id = :batch_id,
                        input_mode = :input_mode,
                        manifest_path = :manifest_path,
                        base_dir = :base_dir,
                        config_dir = :config_dir,
                        profile = :profile,
                        seed = :seed,
                        formats = :formats,
                        resumed_from_run_id = :resumed_from_run_id,
                        status = :status,
                        started_at_utc = :started_at_utc,
                        finished_at_utc = :finished_at_utc,
                        total_records = :total_records,
                        candidate_pair_count = :candidate_pair_count,
                        cluster_count = :cluster_count,
                        golden_record_count = :golden_record_count,
                        review_queue_count = :review_queue_count,
                        failure_detail = :failure_detail,
                        summary_json = :summary_json
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": metadata.run_id,
                    "run_key": metadata.run_key,
                    "attempt_number": metadata.attempt_number,
                    "batch_id": metadata.batch_id,
                    "input_mode": metadata.input_mode,
                    "manifest_path": metadata.manifest_path,
                    "base_dir": metadata.base_dir,
                    "config_dir": metadata.config_dir,
                    "profile": metadata.profile,
                    "seed": metadata.seed,
                    "formats": metadata.formats,
                    "resumed_from_run_id": summary.get("resume", {}).get("resumed_from_run_id", ""),
                    "status": metadata.status,
                    "started_at_utc": metadata.started_at_utc,
                    "finished_at_utc": metadata.finished_at_utc,
                    "total_records": int(summary.get("total_records", 0)),
                    "candidate_pair_count": int(summary.get("candidate_pair_count", 0)),
                    "cluster_count": int(summary.get("cluster_count", 0)),
                    "golden_record_count": int(summary.get("golden_record_count", 0)),
                    "review_queue_count": int(summary.get("review_queue_count", 0)),
                    "failure_detail": metadata.failure_detail or "",
                    "summary_json": json.dumps(summary, sort_keys=True),
                },
            )
            self._persist_artifact_rows(connection, "normalized_source_records", metadata.run_id, normalized_rows)
            self._persist_artifact_rows(connection, "candidate_pairs", metadata.run_id, match_rows)
            self._persist_artifact_rows(connection, "blocking_metrics", metadata.run_id, blocking_metrics_rows)
            self._persist_artifact_rows(connection, "entity_clusters", metadata.run_id, cluster_rows)
            self._persist_artifact_rows(connection, "golden_records", metadata.run_id, golden_rows)
            self._persist_artifact_rows(connection, "source_to_golden_crosswalk", metadata.run_id, crosswalk_rows)
            self._persist_review_case_rows(
                connection,
                metadata.run_id,
                review_rows,
                created_at_utc=metadata.finished_at_utc,
            )

    def update_run_summary(
        self,
        *,
        run_id: str,
        summary: dict[str, object],
    ) -> None:
        with self.engine.begin() as connection:
            cursor = connection.execute(
                text(
                    """
                    UPDATE pipeline_runs
                    SET total_records = :total_records,
                        candidate_pair_count = :candidate_pair_count,
                        cluster_count = :cluster_count,
                        golden_record_count = :golden_record_count,
                        review_queue_count = :review_queue_count,
                        summary_json = :summary_json
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "total_records": int(summary.get("total_records", 0)),
                    "candidate_pair_count": int(summary.get("candidate_pair_count", 0)),
                    "cluster_count": int(summary.get("cluster_count", 0)),
                    "golden_record_count": int(summary.get("golden_record_count", 0)),
                    "review_queue_count": int(summary.get("review_queue_count", 0)),
                    "summary_json": json.dumps(summary, sort_keys=True),
                },
            )
        if cursor.rowcount == 0:
            raise FileNotFoundError(f"Persisted run not found: {run_id}")

    def mark_run_failed(
        self,
        *,
        run_id: str,
        finished_at_utc: str,
        failure_detail: str,
        summary: dict[str, object] | None = None,
    ) -> None:
        resolved_summary = summary or {}
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE pipeline_runs
                    SET status = 'failed',
                        finished_at_utc = :finished_at_utc,
                        failure_detail = :failure_detail,
                        total_records = :total_records,
                        candidate_pair_count = :candidate_pair_count,
                        cluster_count = :cluster_count,
                        golden_record_count = :golden_record_count,
                        review_queue_count = :review_queue_count,
                        summary_json = :summary_json
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "finished_at_utc": finished_at_utc,
                    "failure_detail": failure_detail,
                    "total_records": int(resolved_summary.get("total_records", 0)),
                    "candidate_pair_count": int(resolved_summary.get("candidate_pair_count", 0)),
                    "cluster_count": int(resolved_summary.get("cluster_count", 0)),
                    "golden_record_count": int(resolved_summary.get("golden_record_count", 0)),
                    "review_queue_count": int(resolved_summary.get("review_queue_count", 0)),
                    "summary_json": json.dumps(resolved_summary, sort_keys=True),
                },
            )

    def complete_export_run(
        self,
        *,
        export_run_id: str,
        finished_at_utc: str,
        snapshot_dir: str,
        current_pointer_path: str,
        row_counts: dict[str, int],
        metadata: dict[str, object],
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE export_job_runs
                    SET status = 'completed',
                        finished_at_utc = :finished_at_utc,
                        snapshot_dir = :snapshot_dir,
                        current_pointer_path = :current_pointer_path,
                        row_counts_json = :row_counts_json,
                        metadata_json = :metadata_json,
                        failure_detail = ''
                    WHERE export_run_id = :export_run_id
                    """
                ),
                {
                    "export_run_id": export_run_id,
                    "finished_at_utc": finished_at_utc,
                    "snapshot_dir": snapshot_dir,
                    "current_pointer_path": current_pointer_path,
                    "row_counts_json": json.dumps(row_counts, sort_keys=True),
                    "metadata_json": json.dumps(metadata, sort_keys=True),
                },
            )

    def mark_export_run_failed(
        self,
        *,
        export_run_id: str,
        finished_at_utc: str,
        failure_detail: str,
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE export_job_runs
                    SET status = 'failed',
                        finished_at_utc = :finished_at_utc,
                        failure_detail = :failure_detail
                    WHERE export_run_id = :export_run_id
                    """
                ),
                {
                    "export_run_id": export_run_id,
                    "finished_at_utc": finished_at_utc,
                    "failure_detail": failure_detail,
                },
            )

    def load_run_checkpoint(self, *, run_id: str, stage_name: str) -> RunCheckpointRecord:
        if stage_name not in RUN_CHECKPOINT_STAGE_ORDER:
            raise ValueError(
                f"Unsupported run checkpoint stage {stage_name!r}; expected one of {sorted(RUN_CHECKPOINT_STAGE_ORDER)}"
            )

        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT *
                FROM run_checkpoints
                WHERE run_id = :run_id AND stage_name = :stage_name
                """,
                {"run_id": run_id, "stage_name": stage_name},
            )
        if row is None:
            raise FileNotFoundError(
                f"Persisted run checkpoint not found: run_id={run_id} stage_name={stage_name}"
            )
        return _row_to_run_checkpoint(row)

    def load_latest_run_checkpoint(self, run_id: str) -> RunCheckpointRecord | None:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT *
                FROM run_checkpoints
                WHERE run_id = :run_id
                ORDER BY stage_order DESC, checkpointed_at_utc DESC, checkpoint_id DESC
                LIMIT 1
                """,
                {"run_id": run_id},
            )
        if row is None:
            return None
        return _row_to_run_checkpoint(row)

    def latest_resume_checkpoint_for_run_key(self, run_key: str) -> RunCheckpointRecord | None:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT run_checkpoints.*
                FROM run_checkpoints
                JOIN pipeline_runs
                  ON pipeline_runs.run_id = run_checkpoints.run_id
                WHERE pipeline_runs.run_key = :run_key
                  AND pipeline_runs.status = 'failed'
                ORDER BY pipeline_runs.attempt_number DESC,
                         run_checkpoints.stage_order DESC,
                         run_checkpoints.checkpointed_at_utc DESC,
                         run_checkpoints.checkpoint_id DESC
                LIMIT 1
                """,
                {"run_key": run_key},
            )
        if row is None:
            return None
        return _row_to_run_checkpoint(row)

    def latest_run_id(self) -> str | None:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT run_id
                FROM pipeline_runs
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """
            )
        return None if row is None else str(row["run_id"])

    def latest_completed_run_id(self) -> str | None:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT run_id
                FROM pipeline_runs
                WHERE status = 'completed'
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """
            )
        return None if row is None else str(row["run_id"])

    def latest_completed_run_for_run_key(self, run_key: str) -> PipelineRunRecord | None:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT run_id
                FROM pipeline_runs
                WHERE status = 'completed' AND run_key = :run_key
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """,
                {"run_key": run_key},
            )
        if row is None:
            return None
        return self.load_run_record(str(row["run_id"]))

    def latest_completed_export_run_for_key(self, export_key: str) -> ExportJobRunRecord | None:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT *
                FROM export_job_runs
                WHERE export_key = :export_key AND status = 'completed'
                ORDER BY attempt_number DESC, export_run_id DESC
                LIMIT 1
                """,
                {"export_key": export_key},
            )
        if row is None:
            return None
        return self._row_to_export_job_run_record(row)

    def latest_completed_run_id_with_review_cases(self) -> str | None:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT pipeline_runs.run_id
                FROM pipeline_runs
                WHERE pipeline_runs.status = 'completed'
                  AND EXISTS (
                      SELECT 1
                      FROM review_cases
                      WHERE review_cases.run_id = pipeline_runs.run_id
                  )
                ORDER BY pipeline_runs.finished_at_utc DESC,
                         pipeline_runs.started_at_utc DESC,
                         pipeline_runs.run_id DESC
                LIMIT 1
                """
            )
        return None if row is None else str(row["run_id"])

    def latest_completed_run_for_manifest(
        self,
        *,
        manifest_path: str,
        config_dir: str | None,
    ) -> PipelineRunRecord | None:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT run_id
                FROM pipeline_runs
                WHERE status = 'completed'
                  AND input_mode = 'manifest'
                  AND manifest_path = :manifest_path
                  AND COALESCE(config_dir, '') = COALESCE(:config_dir, '')
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """,
                {"manifest_path": manifest_path, "config_dir": config_dir},
            )
        if row is None:
            return None
        return self.load_run_record(str(row["run_id"]))

    def load_run_record(self, run_id: str) -> PipelineRunRecord:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                "SELECT * FROM pipeline_runs WHERE run_id = :run_id",
                {"run_id": run_id},
            )
        if row is None:
            raise FileNotFoundError(f"Persisted run not found: {run_id}")
        return _row_to_run_record(row)

    def list_run_records(
        self,
        *,
        status: str | None = None,
        input_mode: str | None = None,
        batch_id: str | None = None,
        search_query: str | None = None,
        sort: str = "finished_at_desc",
        limit: int = 50,
        offset: int = 0,
    ) -> PaginatedResult:
        _validate_pagination(limit=limit, offset=offset)
        order_by = RUN_LIST_SORTS.get(sort)
        if order_by is None:
            raise ValueError(
                f"Unsupported run sort {sort!r}; expected one of {sorted(RUN_LIST_SORTS)}"
            )

        filters: list[str] = ["1 = 1"]
        parameters: dict[str, object] = {"limit": limit, "offset": offset}
        if status is not None:
            normalized_status = status.strip().lower()
            if normalized_status not in RUN_STATUSES:
                raise ValueError(
                    f"Unsupported run status {status!r}; expected one of {sorted(RUN_STATUSES)}"
                )
            filters.append("status = :status")
            parameters["status"] = normalized_status
        if input_mode is not None:
            normalized_input_mode = input_mode.strip()
            if not normalized_input_mode:
                raise ValueError("run input_mode filter must be non-empty when provided")
            filters.append("input_mode = :input_mode")
            parameters["input_mode"] = normalized_input_mode
        if batch_id is not None:
            normalized_batch_id = batch_id.strip()
            if not normalized_batch_id:
                raise ValueError("run batch_id filter must be non-empty when provided")
            filters.append("batch_id = :batch_id")
            parameters["batch_id"] = normalized_batch_id

        normalized_query = _normalize_search_query(search_query)
        if normalized_query is not None:
            filters.append(
                "("
                "LOWER(COALESCE(run_id, '')) LIKE :search_query "
                "OR LOWER(COALESCE(run_key, '')) LIKE :search_query "
                "OR LOWER(COALESCE(batch_id, '')) LIKE :search_query "
                "OR LOWER(COALESCE(manifest_path, '')) LIKE :search_query "
                "OR LOWER(COALESCE(base_dir, '')) LIKE :search_query"
                ")"
            )
            parameters["search_query"] = f"%{normalized_query}%"

        where_clause = " AND ".join(filters)
        with self.engine.connect() as connection:
            total_row = self._fetchone(
                connection,
                f"SELECT COUNT(*) AS total FROM pipeline_runs WHERE {where_clause}",
                parameters,
            )
            rows = self._fetchall(
                connection,
                f"""
                SELECT *
                FROM pipeline_runs
                WHERE {where_clause}
                ORDER BY {order_by}
                LIMIT :limit OFFSET :offset
                """,
                parameters,
            )
        total_count = int(total_row["total"] or 0) if total_row is not None else 0
        items = [_row_to_run_record(row) for row in rows]
        return PaginatedResult(
            items=items,
            total_count=total_count,
            next_page_token=_build_next_page_token(
                offset=offset,
                returned_count=len(items),
                total_count=total_count,
            ),
        )

    def _row_to_export_job_run_record(self, row: Mapping[str, object]) -> ExportJobRunRecord:
        return ExportJobRunRecord(
            export_run_id=str(row["export_run_id"]),
            export_key=str(row["export_key"]),
            attempt_number=int(row["attempt_number"] or 0),
            job_name=str(row["job_name"]),
            source_run_id=str(row["source_run_id"]),
            contract_name=str(row["contract_name"]),
            contract_version=str(row["contract_version"]),
            output_root=str(row["output_root"]),
            status=str(row["status"]),
            started_at_utc=str(row["started_at_utc"]),
            finished_at_utc=str(row["finished_at_utc"]),
            snapshot_dir="" if row["snapshot_dir"] is None else str(row["snapshot_dir"]),
            current_pointer_path=""
            if row["current_pointer_path"] is None
            else str(row["current_pointer_path"]),
            row_counts=json.loads(str(row["row_counts_json"] or "{}")),
            metadata=json.loads(str(row["metadata_json"] or "{}")),
            failure_detail=None if row["failure_detail"] in (None, "") else str(row["failure_detail"]),
        )

    def load_export_run_record(self, export_run_id: str) -> ExportJobRunRecord:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                "SELECT * FROM export_job_runs WHERE export_run_id = :export_run_id",
                {"export_run_id": export_run_id},
            )
        if row is None:
            raise FileNotFoundError(f"Persisted export run not found: {export_run_id}")
        return self._row_to_export_job_run_record(row)

    def list_export_runs(
        self,
        *,
        job_name: str | None = None,
        source_run_id: str | None = None,
        status: str | None = None,
    ) -> list[ExportJobRunRecord]:
        filters: list[str] = ["1 = 1"]
        parameters: dict[str, object] = {}
        if job_name is not None:
            filters.append("job_name = :job_name")
            parameters["job_name"] = job_name
        if source_run_id is not None:
            filters.append("source_run_id = :source_run_id")
            parameters["source_run_id"] = source_run_id
        if status is not None:
            normalized_status = status.strip().lower()
            if normalized_status not in EXPORT_RUN_STATUSES:
                raise ValueError(
                    f"Unsupported export run status {status!r}; expected one of {sorted(EXPORT_RUN_STATUSES)}"
                )
            filters.append("status = :status")
            parameters["status"] = normalized_status
        where_clause = " AND ".join(filters)
        with self.engine.connect() as connection:
            rows = self._fetchall(
                connection,
                f"""
                SELECT *
                FROM export_job_runs
                WHERE {where_clause}
                ORDER BY started_at_utc DESC, export_run_id DESC
                """,
                parameters,
            )
        return [self._row_to_export_job_run_record(row) for row in rows]

    def record_audit_event(
        self,
        *,
        actor_type: str,
        actor_id: str,
        action: str,
        resource_type: str,
        resource_id: str,
        status: str,
        run_id: str | None = None,
        details: dict[str, object] | None = None,
        occurred_at_utc: str | None = None,
    ) -> AuditEventRecord:
        normalized_actor_type = actor_type.strip()
        normalized_actor_id = actor_id.strip()
        normalized_action = action.strip()
        normalized_resource_type = resource_type.strip()
        normalized_resource_id = resource_id.strip()
        normalized_status = status.strip().lower()
        sanitized_details = sanitize_observability_fields(details or {})
        if not normalized_actor_type:
            raise ValueError("Audit events require a non-empty actor_type")
        if not normalized_actor_id:
            raise ValueError("Audit events require a non-empty actor_id")
        if not normalized_action:
            raise ValueError("Audit events require a non-empty action")
        if not normalized_resource_type:
            raise ValueError("Audit events require a non-empty resource_type")
        if not normalized_resource_id:
            raise ValueError("Audit events require a non-empty resource_id")
        if normalized_status not in AUDIT_EVENT_STATUSES:
            raise ValueError(
                f"Unsupported audit event status {status!r}; expected one of {sorted(AUDIT_EVENT_STATUSES)}"
            )

        timestamp = occurred_at_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        audit_event_id = build_audit_event_id(datetime.fromisoformat(timestamp.replace("Z", "+00:00")))
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO audit_events (
                        audit_event_id,
                        occurred_at_utc,
                        actor_type,
                        actor_id,
                        action,
                        resource_type,
                        resource_id,
                        run_id,
                        status,
                        details_json
                    ) VALUES (
                        :audit_event_id,
                        :occurred_at_utc,
                        :actor_type,
                        :actor_id,
                        :action,
                        :resource_type,
                        :resource_id,
                        :run_id,
                        :status,
                        :details_json
                    )
                    """
                ),
                {
                    "audit_event_id": audit_event_id,
                    "occurred_at_utc": timestamp,
                    "actor_type": normalized_actor_type,
                    "actor_id": normalized_actor_id,
                    "action": normalized_action,
                    "resource_type": normalized_resource_type,
                    "resource_id": normalized_resource_id,
                    "run_id": run_id,
                    "status": normalized_status,
                    "details_json": json.dumps(sanitized_details, sort_keys=True),
                },
            )
        return self.load_audit_event(audit_event_id)

    def load_audit_event(self, audit_event_id: str) -> AuditEventRecord:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                "SELECT * FROM audit_events WHERE audit_event_id = :audit_event_id",
                {"audit_event_id": audit_event_id},
            )
        if row is None:
            raise FileNotFoundError(f"Persisted audit event not found: {audit_event_id}")
        return _row_to_audit_event(row)

    def list_audit_events(
        self,
        *,
        run_id: str | None = None,
        action: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[AuditEventRecord]:
        if limit <= 0:
            raise ValueError("Audit event limit must be greater than 0")
        filters: list[str] = ["1 = 1"]
        parameters: dict[str, object] = {"limit": limit}
        if run_id is not None:
            filters.append("run_id = :run_id")
            parameters["run_id"] = run_id
        if action is not None:
            filters.append("action = :action")
            parameters["action"] = action.strip()
        if status is not None:
            normalized_status = status.strip().lower()
            if normalized_status not in AUDIT_EVENT_STATUSES:
                raise ValueError(
                    f"Unsupported audit event status {status!r}; expected one of {sorted(AUDIT_EVENT_STATUSES)}"
                )
            filters.append("status = :status")
            parameters["status"] = normalized_status
        where_clause = " AND ".join(filters)
        with self.engine.connect() as connection:
            rows = self._fetchall(
                connection,
                f"""
                SELECT *
                FROM audit_events
                WHERE {where_clause}
                ORDER BY occurred_at_utc DESC, audit_event_id DESC
                LIMIT :limit
                """,
                parameters,
            )
        return [_row_to_audit_event(row) for row in rows]

    def load_operational_metrics(self) -> StoreOperationalMetrics:
        with self.engine.connect() as connection:
            run_status_counts = {status: 0 for status in RUN_STATUSES}
            for row in self._fetchall(connection, "SELECT status, COUNT(*) AS total FROM pipeline_runs GROUP BY status"):
                status = str(row["status"])
                if status in run_status_counts:
                    run_status_counts[status] = int(row["total"] or 0)

            export_status_counts = {status: 0 for status in EXPORT_RUN_STATUSES}
            for row in self._fetchall(connection, "SELECT status, COUNT(*) AS total FROM export_job_runs GROUP BY status"):
                status = str(row["status"])
                if status in export_status_counts:
                    export_status_counts[status] = int(row["total"] or 0)

            review_case_status_counts = {status: 0 for status in REVIEW_CASE_STATUSES}
            for row in self._fetchall(connection, "SELECT queue_status, COUNT(*) AS total FROM review_cases GROUP BY queue_status"):
                status = str(row["queue_status"])
                if status in review_case_status_counts:
                    review_case_status_counts[status] = int(row["total"] or 0)

            audit_event_row = self._fetchone(connection, "SELECT COUNT(*) AS total FROM audit_events")
            latest_completed_row = self._fetchone(
                connection,
                """
                SELECT run_id, finished_at_utc
                FROM pipeline_runs
                WHERE status = 'completed'
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """
            )
            latest_failed_row = self._fetchone(
                connection,
                """
                SELECT run_id, finished_at_utc
                FROM pipeline_runs
                WHERE status = 'failed'
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """
            )

        return StoreOperationalMetrics(
            run_status_counts=run_status_counts,
            export_status_counts=export_status_counts,
            review_case_status_counts=review_case_status_counts,
            audit_event_count=int(audit_event_row["total"] or 0),
            latest_completed_run_id=None
            if latest_completed_row is None
            else str(latest_completed_row["run_id"]),
            latest_completed_run_finished_at_utc=None
            if latest_completed_row is None
            else str(latest_completed_row["finished_at_utc"]),
            latest_failed_run_id=None if latest_failed_row is None else str(latest_failed_row["run_id"]),
            latest_failed_run_finished_at_utc=None
            if latest_failed_row is None
            else str(latest_failed_row["finished_at_utc"]),
        )

    def _load_artifact_rows(self, table_name: str, run_id: str) -> list[dict[str, str]]:
        headers = ARTIFACT_HEADERS[table_name]
        select_columns = ", ".join(f'"{header}"' for header in headers)
        with self.engine.connect() as connection:
            rows = self._fetchall(
                connection,
                f"""
                SELECT {select_columns}
                FROM {table_name}
                WHERE run_id = :run_id
                ORDER BY row_index ASC
                """,
                {"run_id": run_id},
            )
        return [_row_to_strings(row, headers) for row in rows]

    def _load_single_artifact_row(
        self,
        table_name: str,
        *,
        run_id: str,
        filters: dict[str, str],
    ) -> dict[str, str]:
        headers = ARTIFACT_HEADERS[table_name]
        select_columns = ", ".join(f'"{header}"' for header in headers)
        where_parts = ["run_id = :run_id"]
        parameters: dict[str, object] = {"run_id": run_id}
        for index, (column, value) in enumerate(filters.items()):
            parameter_name = f"filter_{index}"
            where_parts.append(f"{column} = :{parameter_name}")
            parameters[parameter_name] = value
        where_clause = " AND ".join(where_parts)

        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                f"""
                SELECT {select_columns}
                FROM {table_name}
                WHERE {where_clause}
                ORDER BY row_index ASC
                LIMIT 1
                """,
                parameters,
            )
        if row is None:
            filter_text = ", ".join(f"{column}={value}" for column, value in filters.items())
            raise FileNotFoundError(
                f"Persisted {table_name} row not found: run_id={run_id} {filter_text}"
            )
        return _row_to_strings(row, headers)

    def load_run_bundle(self, run_id: str) -> PersistedRunBundle:
        run = self.load_run_record(run_id)
        return PersistedRunBundle(
            run=run,
            normalized_rows=self._load_artifact_rows("normalized_source_records", run_id),
            candidate_pairs=self._load_artifact_rows("candidate_pairs", run_id),
            blocking_metrics_rows=self._load_artifact_rows("blocking_metrics", run_id),
            cluster_rows=self._load_artifact_rows("entity_clusters", run_id),
            golden_rows=self._load_artifact_rows("golden_records", run_id),
            crosswalk_rows=self._load_artifact_rows("source_to_golden_crosswalk", run_id),
            review_rows=self._load_artifact_rows("review_cases", run_id),
        )

    def load_golden_record(
        self,
        *,
        run_id: str,
        golden_id: str,
    ) -> dict[str, str]:
        return self._load_single_artifact_row(
            "golden_records",
            run_id=run_id,
            filters={"golden_id": golden_id},
        )

    def list_golden_records(
        self,
        *,
        run_id: str,
        cluster_id: str | None = None,
        person_entity_id: str | None = None,
        search_query: str | None = None,
        sort: str = "golden_id_asc",
        limit: int = 50,
        offset: int = 0,
    ) -> PaginatedResult:
        _validate_pagination(limit=limit, offset=offset)
        order_by = GOLDEN_LIST_SORTS.get(sort)
        if order_by is None:
            raise ValueError(
                f"Unsupported golden-record sort {sort!r}; expected one of {sorted(GOLDEN_LIST_SORTS)}"
            )

        headers = ARTIFACT_HEADERS["golden_records"]
        select_columns = ", ".join(f'"{header}"' for header in headers)
        filters: list[str] = ["run_id = :run_id"]
        parameters: dict[str, object] = {"run_id": run_id, "limit": limit, "offset": offset}
        if cluster_id is not None:
            normalized_cluster_id = cluster_id.strip()
            if not normalized_cluster_id:
                raise ValueError("golden-record cluster_id filter must be non-empty when provided")
            filters.append("cluster_id = :cluster_id")
            parameters["cluster_id"] = normalized_cluster_id
        if person_entity_id is not None:
            normalized_person_entity_id = person_entity_id.strip()
            if not normalized_person_entity_id:
                raise ValueError("golden-record person_entity_id filter must be non-empty when provided")
            filters.append("person_entity_id = :person_entity_id")
            parameters["person_entity_id"] = normalized_person_entity_id

        normalized_query = _normalize_search_query(search_query)
        if normalized_query is not None:
            filters.append(
                "("
                "LOWER(COALESCE(golden_id, '')) LIKE :search_query "
                "OR LOWER(COALESCE(cluster_id, '')) LIKE :search_query "
                "OR LOWER(COALESCE(person_entity_id, '')) LIKE :search_query "
                "OR LOWER(COALESCE(first_name, '')) LIKE :search_query "
                "OR LOWER(COALESCE(last_name, '')) LIKE :search_query"
                ")"
            )
            parameters["search_query"] = f"%{normalized_query}%"

        where_clause = " AND ".join(filters)
        with self.engine.connect() as connection:
            total_row = self._fetchone(
                connection,
                f"SELECT COUNT(*) AS total FROM golden_records WHERE {where_clause}",
                parameters,
            )
            rows = self._fetchall(
                connection,
                f"""
                SELECT {select_columns}
                FROM golden_records
                WHERE {where_clause}
                ORDER BY {order_by}
                LIMIT :limit OFFSET :offset
                """,
                parameters,
            )
        total_count = int(total_row["total"] or 0) if total_row is not None else 0
        items = [_row_to_strings(row, headers) for row in rows]
        return PaginatedResult(
            items=items,
            total_count=total_count,
            next_page_token=_build_next_page_token(
                offset=offset,
                returned_count=len(items),
                total_count=total_count,
            ),
        )

    def load_crosswalk_record_for_source(
        self,
        *,
        run_id: str,
        source_record_id: str,
    ) -> dict[str, str]:
        return self._load_single_artifact_row(
            "source_to_golden_crosswalk",
            run_id=run_id,
            filters={"source_record_id": source_record_id},
        )

    def list_review_cases(
        self,
        *,
        run_id: str,
        queue_status: str | None = None,
        assigned_to: str | None = None,
        search_query: str | None = None,
        sort: str = "queue_order_asc",
        limit: int | None = None,
        offset: int = 0,
    ) -> list[PersistedReviewCase] | PaginatedResult:
        if limit is not None:
            _validate_pagination(limit=limit, offset=offset)
            order_by = REVIEW_CASE_LIST_SORTS.get(sort)
            if order_by is None:
                raise ValueError(
                    f"Unsupported review-case sort {sort!r}; expected one of {sorted(REVIEW_CASE_LIST_SORTS)}"
                )
        else:
            if offset != 0:
                raise ValueError("Review-case offset requires limit to be provided")
            order_by = "row_index ASC"

        filters: list[str] = ["run_id = :run_id"]
        parameters: dict[str, object] = {"run_id": run_id}
        if queue_status is not None:
            filters.append("queue_status = :queue_status")
            parameters["queue_status"] = validate_review_case_status(queue_status)
        if assigned_to is not None:
            normalized_assigned_to = assigned_to.strip()
            if not normalized_assigned_to:
                raise ValueError("review-case assigned_to filter must be non-empty when provided")
            filters.append("assigned_to = :assigned_to")
            parameters["assigned_to"] = normalized_assigned_to
        normalized_query = _normalize_search_query(search_query)
        if normalized_query is not None:
            filters.append(
                "("
                "LOWER(COALESCE(review_id, '')) LIKE :search_query "
                "OR LOWER(COALESCE(left_id, '')) LIKE :search_query "
                "OR LOWER(COALESCE(right_id, '')) LIKE :search_query "
                "OR LOWER(COALESCE(assigned_to, '')) LIKE :search_query "
                "OR LOWER(COALESCE(operator_notes, '')) LIKE :search_query"
                ")"
            )
            parameters["search_query"] = f"%{normalized_query}%"
        where_clause = " AND ".join(filters)
        with self.engine.connect() as connection:
            total_row = None
            if limit is not None:
                total_row = self._fetchone(
                    connection,
                    f"SELECT COUNT(*) AS total FROM review_cases WHERE {where_clause}",
                    parameters,
                )
            rows = self._fetchall(
                connection,
                f"""
                SELECT run_id, review_id, left_id, right_id, score, reason_codes,
                       top_contributing_match_signals, queue_status, assigned_to,
                       operator_notes, created_at_utc, updated_at_utc, resolved_at_utc
                FROM review_cases
                WHERE {where_clause}
                ORDER BY {order_by}
                {"LIMIT :limit OFFSET :offset" if limit is not None else ""}
                """,
                (
                    parameters
                    if limit is None
                    else {
                        **parameters,
                        "limit": limit,
                        "offset": offset,
                    }
                ),
            )
        items = [_row_to_review_case(row) for row in rows]
        if limit is None:
            return items

        total_count = int(total_row["total"] or 0) if total_row is not None else 0
        return PaginatedResult(
            items=items,
            total_count=total_count,
            next_page_token=_build_next_page_token(
                offset=offset,
                returned_count=len(items),
                total_count=total_count,
            ),
        )

    def load_review_case(self, *, run_id: str, review_id: str) -> PersistedReviewCase:
        with self.engine.connect() as connection:
            row = self._fetchone(
                connection,
                """
                SELECT run_id, review_id, left_id, right_id, score, reason_codes,
                       top_contributing_match_signals, queue_status, assigned_to,
                       operator_notes, created_at_utc, updated_at_utc, resolved_at_utc
                FROM review_cases
                WHERE run_id = :run_id AND review_id = :review_id
                """,
                {"run_id": run_id, "review_id": review_id},
            )
        if row is None:
            raise FileNotFoundError(f"Persisted review case not found: run_id={run_id} review_id={review_id}")
        return _row_to_review_case(row)

    def update_review_case(
        self,
        *,
        run_id: str,
        review_id: str,
        queue_status: str | None = None,
        assigned_to: str | None = None,
        operator_notes: str | None = None,
        updated_at_utc: str | None = None,
    ) -> PersistedReviewCase:
        existing = self.load_review_case(run_id=run_id, review_id=review_id)
        timestamp = updated_at_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        next_status = existing.queue_status
        if queue_status is not None:
            next_status = validate_review_case_transition(existing.queue_status, queue_status)

        next_assigned_to = existing.assigned_to if assigned_to is None else assigned_to.strip()
        next_operator_notes = existing.operator_notes if operator_notes is None else operator_notes.strip()
        next_resolved_at = existing.resolved_at_utc
        if next_status in REVIEW_CASE_STATUSES and next_status in {"approved", "rejected"}:
            next_resolved_at = existing.resolved_at_utc or timestamp
        elif next_status in {"pending", "deferred"}:
            next_resolved_at = ""

        with self.engine.begin() as connection:
            cursor = connection.execute(
                text(
                    """
                    UPDATE review_cases
                    SET queue_status = :queue_status,
                        assigned_to = :assigned_to,
                        operator_notes = :operator_notes,
                        updated_at_utc = :updated_at_utc,
                        resolved_at_utc = :resolved_at_utc
                    WHERE run_id = :run_id AND review_id = :review_id
                    """
                ),
                {
                    "queue_status": next_status,
                    "assigned_to": next_assigned_to,
                    "operator_notes": next_operator_notes,
                    "updated_at_utc": timestamp,
                    "resolved_at_utc": next_resolved_at,
                    "run_id": run_id,
                    "review_id": review_id,
                },
            )
        if cursor.rowcount == 0:
            raise FileNotFoundError(f"Persisted review case not found: run_id={run_id} review_id={review_id}")
        return self.load_review_case(run_id=run_id, review_id=review_id)


PipelineStateStore = SQLitePipelineStore
