"""SQLite-backed persistence for pipeline run state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from etl_identity_engine.review_cases import (
    REVIEW_CASE_STATUSES,
    validate_review_case_status,
    validate_review_case_transition,
)
from etl_identity_engine.storage.migration_runner import upgrade_sqlite_store
from etl_identity_engine.output_contracts import (
    BLOCKING_METRICS_HEADERS,
    CROSSWALK_HEADERS,
    ENTITY_CLUSTER_HEADERS,
    GOLDEN_HEADERS,
    MANUAL_REVIEW_HEADERS,
    MATCH_SCORE_HEADERS,
    NORMALIZED_HEADERS,
)


RUN_STATUSES = frozenset({"running", "completed", "failed"})
EXPORT_RUN_STATUSES = frozenset({"running", "completed", "failed"})
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

PIPELINE_STATE_TABLES = ("pipeline_runs", "export_job_runs", *ARTIFACT_TABLE_NAMES)


def build_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"RUN-{timestamp}-{uuid4().hex[:8].upper()}"


def build_export_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"EXP-{timestamp}-{uuid4().hex[:8].upper()}"


def build_run_key(
    *,
    input_mode: str,
    manifest_path: str | None,
    batch_id: str | None,
    config_dir: str | None,
    profile: str | None,
    seed: int | None,
    duplicate_rate: float | None,
    formats: str | None,
    refresh_mode: str | None,
) -> str:
    payload = json.dumps(
        {
            "input_mode": input_mode,
            "manifest_path": manifest_path,
            "batch_id": batch_id,
            "config_dir": config_dir,
            "profile": profile,
            "seed": seed,
            "duplicate_rate": duplicate_rate,
            "formats": formats,
            "refresh_mode": refresh_mode,
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


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def bootstrap_sqlite_store(db_path: Path) -> None:
    upgrade_sqlite_store(Path(db_path))


def _row_to_strings(row: sqlite3.Row, headers: tuple[str, ...]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for header in headers:
        value = row[header]
        resolved[header] = "" if value is None else str(value)
    return resolved


def _row_to_review_case(row: sqlite3.Row) -> PersistedReviewCase:
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


class SQLitePipelineStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        bootstrap_sqlite_store(self.db_path)

    def _clear_existing_run(self, connection: sqlite3.Connection, run_id: str) -> None:
        for table_name in ARTIFACT_TABLE_NAMES:
            connection.execute(f"DELETE FROM {table_name} WHERE run_id = ?", (run_id,))
        
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
        with _connect(self.db_path) as connection:
            completed = connection.execute(
                """
                SELECT run_id, attempt_number
                FROM pipeline_runs
                WHERE run_key = ? AND status = 'completed'
                ORDER BY attempt_number DESC
                LIMIT 1
                """,
                (run_key,),
            ).fetchone()
            if completed is not None:
                return RunStartDecision(
                    action="reuse_completed",
                    run_id=str(completed["run_id"]),
                    run_key=run_key,
                    attempt_number=int(completed["attempt_number"] or 1),
                    started_at_utc=started_at_utc,
                )

            attempt_row = connection.execute(
                "SELECT COALESCE(MAX(attempt_number), 0) AS max_attempt FROM pipeline_runs WHERE run_key = ?",
                (run_key,),
            ).fetchone()
            attempt_number = int(attempt_row["max_attempt"] or 0) + 1
            run_id = build_run_id(datetime.fromisoformat(started_at_utc.replace("Z", "+00:00")))
            connection.execute(
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, '', 0, 0, 0, 0, 0, '', '{}')
                """,
                (
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
                    started_at_utc,
                ),
            )
            connection.commit()

        return RunStartDecision(
            action="start_new",
            run_id=run_id,
            run_key=run_key,
            attempt_number=attempt_number,
            started_at_utc=started_at_utc,
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
        with _connect(self.db_path) as connection:
            completed = connection.execute(
                """
                SELECT export_run_id, attempt_number
                FROM export_job_runs
                WHERE export_key = ? AND status = 'completed'
                ORDER BY attempt_number DESC
                LIMIT 1
                """,
                (export_key,),
            ).fetchone()
            if completed is not None:
                return ExportStartDecision(
                    action="reuse_completed",
                    export_run_id=str(completed["export_run_id"]),
                    export_key=export_key,
                    attempt_number=int(completed["attempt_number"] or 1),
                    started_at_utc=started_at_utc,
                )

            attempt_row = connection.execute(
                "SELECT COALESCE(MAX(attempt_number), 0) AS max_attempt FROM export_job_runs WHERE export_key = ?",
                (export_key,),
            ).fetchone()
            attempt_number = int(attempt_row["max_attempt"] or 0) + 1
            export_run_id = build_export_run_id(
                datetime.fromisoformat(started_at_utc.replace("Z", "+00:00"))
            )
            connection.execute(
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, '', '', '', '{}', '{}', '')
                """,
                (
                    export_run_id,
                    export_key,
                    attempt_number,
                    job_name,
                    source_run_id,
                    contract_name,
                    contract_version,
                    output_root,
                    started_at_utc,
                ),
            )
            connection.commit()

        return ExportStartDecision(
            action="start_new",
            export_run_id=export_run_id,
            export_key=export_key,
            attempt_number=attempt_number,
            started_at_utc=started_at_utc,
        )

    def _persist_artifact_rows(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        run_id: str,
        rows: list[dict[str, object]],
    ) -> None:
        headers = ARTIFACT_HEADERS[table_name]
        placeholders = ", ".join("?" for _ in ("run_id", "row_index", *headers))
        quoted_columns = ", ".join(f'"{column}"' for column in ("run_id", "row_index", *headers))
        connection.executemany(
            f"INSERT INTO {table_name} ({quoted_columns}) VALUES ({placeholders})",
            [
                (
                    run_id,
                    index,
                    *[row.get(header, "") for header in headers],
                )
                for index, row in enumerate(rows)
            ],
        )

    def _persist_review_case_rows(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        rows: list[dict[str, str | float]],
        *,
        created_at_utc: str,
    ) -> None:
        connection.executemany(
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    index,
                    row.get("review_id", ""),
                    row.get("left_id", ""),
                    row.get("right_id", ""),
                    row.get("score", ""),
                    row.get("reason_codes", ""),
                    row.get("top_contributing_match_signals", ""),
                    validate_review_case_status(str(row.get("queue_status", "pending") or "pending")),
                    str(row.get("assigned_to", "") or ""),
                    str(row.get("operator_notes", "") or ""),
                    str(row.get("created_at_utc", "") or created_at_utc),
                    str(row.get("updated_at_utc", "") or created_at_utc),
                    str(row.get("resolved_at_utc", "") or ""),
                )
                for index, row in enumerate(rows)
            ],
        )

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

        with _connect(self.db_path) as connection:
            self._clear_existing_run(connection, metadata.run_id)
            connection.execute(
                """
                UPDATE pipeline_runs
                SET run_key = ?,
                    attempt_number = ?,
                    batch_id = ?,
                    input_mode = ?,
                    manifest_path = ?,
                    base_dir = ?,
                    config_dir = ?,
                    profile = ?,
                    seed = ?,
                    formats = ?,
                    status = ?,
                    started_at_utc = ?,
                    finished_at_utc = ?,
                    total_records = ?,
                    candidate_pair_count = ?,
                    cluster_count = ?,
                    golden_record_count = ?,
                    review_queue_count = ?,
                    failure_detail = ?,
                    summary_json = ?
                WHERE run_id = ?
                """,
                (
                    metadata.run_key,
                    metadata.attempt_number,
                    metadata.batch_id,
                    metadata.input_mode,
                    metadata.manifest_path,
                    metadata.base_dir,
                    metadata.config_dir,
                    metadata.profile,
                    metadata.seed,
                    metadata.formats,
                    metadata.status,
                    metadata.started_at_utc,
                    metadata.finished_at_utc,
                    int(summary.get("total_records", 0)),
                    int(summary.get("candidate_pair_count", 0)),
                    int(summary.get("cluster_count", 0)),
                    int(summary.get("golden_record_count", 0)),
                    int(summary.get("review_queue_count", 0)),
                    metadata.failure_detail or "",
                    json.dumps(summary, sort_keys=True),
                    metadata.run_id,
                ),
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
            connection.commit()

    def mark_run_failed(
        self,
        *,
        run_id: str,
        finished_at_utc: str,
        failure_detail: str,
    ) -> None:
        with _connect(self.db_path) as connection:
            connection.execute(
                """
                UPDATE pipeline_runs
                SET status = 'failed',
                    finished_at_utc = ?,
                    failure_detail = ?,
                    summary_json = '{}'
                WHERE run_id = ?
                """,
                (finished_at_utc, failure_detail, run_id),
            )
            connection.commit()

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
        with _connect(self.db_path) as connection:
            connection.execute(
                """
                UPDATE export_job_runs
                SET status = 'completed',
                    finished_at_utc = ?,
                    snapshot_dir = ?,
                    current_pointer_path = ?,
                    row_counts_json = ?,
                    metadata_json = ?,
                    failure_detail = ''
                WHERE export_run_id = ?
                """,
                (
                    finished_at_utc,
                    snapshot_dir,
                    current_pointer_path,
                    json.dumps(row_counts, sort_keys=True),
                    json.dumps(metadata, sort_keys=True),
                    export_run_id,
                ),
            )
            connection.commit()

    def mark_export_run_failed(
        self,
        *,
        export_run_id: str,
        finished_at_utc: str,
        failure_detail: str,
    ) -> None:
        with _connect(self.db_path) as connection:
            connection.execute(
                """
                UPDATE export_job_runs
                SET status = 'failed',
                    finished_at_utc = ?,
                    failure_detail = ?
                WHERE export_run_id = ?
                """,
                (finished_at_utc, failure_detail, export_run_id),
            )
            connection.commit()

    def latest_run_id(self) -> str | None:
        with _connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT run_id
                FROM pipeline_runs
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """
            ).fetchone()
        return None if row is None else str(row["run_id"])

    def latest_completed_run_id(self) -> str | None:
        with _connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT run_id
                FROM pipeline_runs
                WHERE status = 'completed'
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """
            ).fetchone()
        return None if row is None else str(row["run_id"])

    def latest_completed_run_for_run_key(self, run_key: str) -> PipelineRunRecord | None:
        with _connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT run_id
                FROM pipeline_runs
                WHERE status = 'completed' AND run_key = ?
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """,
                (run_key,),
            ).fetchone()
        if row is None:
            return None
        return self.load_run_record(str(row["run_id"]))

    def latest_completed_export_run_for_key(self, export_key: str) -> ExportJobRunRecord | None:
        with _connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT *
                FROM export_job_runs
                WHERE export_key = ? AND status = 'completed'
                ORDER BY attempt_number DESC, export_run_id DESC
                LIMIT 1
                """,
                (export_key,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_export_job_run_record(row)

    def latest_completed_run_id_with_review_cases(self) -> str | None:
        with _connect(self.db_path) as connection:
            row = connection.execute(
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
            ).fetchone()
        return None if row is None else str(row["run_id"])

    def latest_completed_run_for_manifest(
        self,
        *,
        manifest_path: str,
        config_dir: str | None,
    ) -> PipelineRunRecord | None:
        with _connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT run_id
                FROM pipeline_runs
                WHERE status = 'completed'
                  AND input_mode = 'manifest'
                  AND manifest_path = ?
                  AND COALESCE(config_dir, '') = COALESCE(?, '')
                ORDER BY finished_at_utc DESC, started_at_utc DESC, run_id DESC
                LIMIT 1
                """,
                (manifest_path, config_dir),
            ).fetchone()
        if row is None:
            return None
        return self.load_run_record(str(row["run_id"]))

    def load_run_record(self, run_id: str) -> PipelineRunRecord:
        with _connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT * FROM pipeline_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"Persisted run not found: {run_id}")

        return PipelineRunRecord(
            run_id=str(row["run_id"]),
            run_key="" if row["run_key"] is None else str(row["run_key"]),
            attempt_number=int(row["attempt_number"] or 0),
            batch_id=row["batch_id"],
            input_mode=str(row["input_mode"]),
            manifest_path=row["manifest_path"],
            base_dir=str(row["base_dir"]),
            config_dir=row["config_dir"],
            profile=row["profile"],
            seed=row["seed"],
            formats=row["formats"],
            status=str(row["status"]),
            started_at_utc=str(row["started_at_utc"]),
            finished_at_utc=str(row["finished_at_utc"]),
            total_records=int(row["total_records"]),
            candidate_pair_count=int(row["candidate_pair_count"]),
            cluster_count=int(row["cluster_count"]),
            golden_record_count=int(row["golden_record_count"]),
            review_queue_count=int(row["review_queue_count"]),
            failure_detail=None if row["failure_detail"] in (None, "") else str(row["failure_detail"]),
            summary=json.loads(str(row["summary_json"])),
        )

    def _row_to_export_job_run_record(self, row: sqlite3.Row) -> ExportJobRunRecord:
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
        with _connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT * FROM export_job_runs WHERE export_run_id = ?",
                (export_run_id,),
            ).fetchone()
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
        parameters: list[str] = []
        if job_name is not None:
            filters.append("job_name = ?")
            parameters.append(job_name)
        if source_run_id is not None:
            filters.append("source_run_id = ?")
            parameters.append(source_run_id)
        if status is not None:
            normalized_status = status.strip().lower()
            if normalized_status not in EXPORT_RUN_STATUSES:
                raise ValueError(
                    f"Unsupported export run status {status!r}; expected one of {sorted(EXPORT_RUN_STATUSES)}"
                )
            filters.append("status = ?")
            parameters.append(normalized_status)
        where_clause = " AND ".join(filters)
        with _connect(self.db_path) as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM export_job_runs
                WHERE {where_clause}
                ORDER BY started_at_utc DESC, export_run_id DESC
                """,
                parameters,
            ).fetchall()
        return [self._row_to_export_job_run_record(row) for row in rows]

    def _load_artifact_rows(self, table_name: str, run_id: str) -> list[dict[str, str]]:
        headers = ARTIFACT_HEADERS[table_name]
        with _connect(self.db_path) as connection:
            rows = connection.execute(
                f"""
                SELECT {", ".join(headers)}
                FROM {table_name}
                WHERE run_id = ?
                ORDER BY row_index ASC
                """,
                (run_id,),
            ).fetchall()
        return [_row_to_strings(row, headers) for row in rows]

    def _load_single_artifact_row(
        self,
        table_name: str,
        *,
        run_id: str,
        filters: dict[str, str],
    ) -> dict[str, str]:
        headers = ARTIFACT_HEADERS[table_name]
        where_parts = ["run_id = ?"]
        parameters: list[str] = [run_id]
        for column, value in filters.items():
            where_parts.append(f"{column} = ?")
            parameters.append(value)
        where_clause = " AND ".join(where_parts)

        with _connect(self.db_path) as connection:
            row = connection.execute(
                f"""
                SELECT {", ".join(headers)}
                FROM {table_name}
                WHERE {where_clause}
                ORDER BY row_index ASC
                LIMIT 1
                """,
                parameters,
            ).fetchone()
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
    ) -> list[PersistedReviewCase]:
        filters: list[str] = ["run_id = ?"]
        parameters: list[str] = [run_id]
        if queue_status is not None:
            filters.append("queue_status = ?")
            parameters.append(validate_review_case_status(queue_status))
        if assigned_to is not None:
            filters.append("assigned_to = ?")
            parameters.append(assigned_to.strip())
        where_clause = " AND ".join(filters)
        with _connect(self.db_path) as connection:
            rows = connection.execute(
                f"""
                SELECT run_id, review_id, left_id, right_id, score, reason_codes,
                       top_contributing_match_signals, queue_status, assigned_to,
                       operator_notes, created_at_utc, updated_at_utc, resolved_at_utc
                FROM review_cases
                WHERE {where_clause}
                ORDER BY row_index ASC
                """,
                parameters,
            ).fetchall()
        return [_row_to_review_case(row) for row in rows]

    def load_review_case(self, *, run_id: str, review_id: str) -> PersistedReviewCase:
        with _connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT run_id, review_id, left_id, right_id, score, reason_codes,
                       top_contributing_match_signals, queue_status, assigned_to,
                       operator_notes, created_at_utc, updated_at_utc, resolved_at_utc
                FROM review_cases
                WHERE run_id = ? AND review_id = ?
                """,
                (run_id, review_id),
            ).fetchone()
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

        with _connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                UPDATE review_cases
                SET queue_status = ?,
                    assigned_to = ?,
                    operator_notes = ?,
                    updated_at_utc = ?,
                    resolved_at_utc = ?
                WHERE run_id = ? AND review_id = ?
                """,
                (
                    next_status,
                    next_assigned_to,
                    next_operator_notes,
                    timestamp,
                    next_resolved_at,
                    run_id,
                    review_id,
                ),
            )
            connection.commit()
        if cursor.rowcount == 0:
            raise FileNotFoundError(f"Persisted review case not found: run_id={run_id} review_id={review_id}")
        return self.load_review_case(run_id=run_id, review_id=review_id)
