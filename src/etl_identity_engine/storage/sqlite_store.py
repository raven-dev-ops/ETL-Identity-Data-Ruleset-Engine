"""SQLite-backed persistence for pipeline run state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from uuid import uuid4

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
    "review_cases": (("review_id",), ("queue_status",), ("left_id",), ("right_id",)),
}

PIPELINE_STATE_TABLES = ("pipeline_runs", *ARTIFACT_TABLE_NAMES)


def build_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"RUN-{timestamp}-{uuid4().hex[:8].upper()}"


def build_run_key(
    *,
    input_mode: str,
    manifest_path: str | None,
    config_dir: str | None,
    profile: str | None,
    seed: int | None,
    duplicate_rate: float | None,
    formats: str | None,
) -> str:
    payload = json.dumps(
        {
            "input_mode": input_mode,
            "manifest_path": manifest_path,
            "config_dir": config_dir,
            "profile": profile,
            "seed": seed,
            "duplicate_rate": duplicate_rate,
            "formats": formats,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()
    return f"RK-{digest}"


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _create_artifact_table(
    connection: sqlite3.Connection,
    table_name: str,
    schema: tuple[tuple[str, str], ...],
) -> None:
    column_sql = ", ".join(f'"{name}" {column_type}' for name, column_type in schema)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            {column_sql},
            PRIMARY KEY (run_id, row_index),
            FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
        )
        """
    )


def bootstrap_sqlite_store(db_path: Path) -> None:
    with _connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                run_key TEXT,
                attempt_number INTEGER,
                batch_id TEXT,
                input_mode TEXT NOT NULL,
                manifest_path TEXT,
                base_dir TEXT NOT NULL,
                config_dir TEXT,
                profile TEXT,
                seed INTEGER,
                formats TEXT,
                status TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT NOT NULL,
                total_records INTEGER NOT NULL,
                candidate_pair_count INTEGER NOT NULL,
                cluster_count INTEGER NOT NULL,
                golden_record_count INTEGER NOT NULL,
                review_queue_count INTEGER NOT NULL,
                failure_detail TEXT,
                summary_json TEXT NOT NULL
            )
            """
        )
        _ensure_pipeline_runs_columns(connection)
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_completed_run_key
            ON pipeline_runs (run_key)
            WHERE status = 'completed'
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_running_run_key
            ON pipeline_runs (run_key)
            WHERE status = 'running'
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_key
            ON pipeline_runs (run_key, attempt_number)
            """
        )

        for table_name, schema in ARTIFACT_SCHEMAS.items():
            _create_artifact_table(connection, table_name, schema)
            for columns in ARTIFACT_INDEXES.get(table_name, ()):
                column_suffix = "_".join(columns)
                index_name = f"idx_{table_name}_{column_suffix}"
                joined_columns = ", ".join(columns)
                connection.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {index_name}
                    ON {table_name} (run_id, {joined_columns})
                    """
                )

        connection.commit()


def _ensure_pipeline_runs_columns(connection: sqlite3.Connection) -> None:
    rows = connection.execute("PRAGMA table_info(pipeline_runs)").fetchall()
    existing_columns = {str(row["name"]) for row in rows}
    required_columns = {
        "run_key": "TEXT",
        "attempt_number": "INTEGER",
        "failure_detail": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE pipeline_runs ADD COLUMN {column_name} {column_type}"
            )


def _row_to_strings(row: sqlite3.Row, headers: tuple[str, ...]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for header in headers:
        value = row[header]
        resolved[header] = "" if value is None else str(value)
    return resolved


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
            self._persist_artifact_rows(connection, "review_cases", metadata.run_id, review_rows)
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
