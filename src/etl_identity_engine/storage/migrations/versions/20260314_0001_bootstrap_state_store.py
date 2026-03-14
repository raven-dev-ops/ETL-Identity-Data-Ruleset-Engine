"""Bootstrap persisted pipeline state schema."""

from __future__ import annotations

from alembic import op
from sqlalchemy import inspect

from etl_identity_engine.output_contracts import (
    CROSSWALK_HEADERS,
    ENTITY_CLUSTER_HEADERS,
    GOLDEN_HEADERS,
    NORMALIZED_HEADERS,
)


revision = "20260314_0001"
down_revision = None
branch_labels = None
depends_on = None


ARTIFACT_TABLE_SPECS: dict[str, tuple[tuple[str, str], ...]] = {
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

ARTIFACT_INDEXES: dict[str, tuple[tuple[str, ...], ...]] = {
    "normalized_source_records": (("source_record_id",), ("person_entity_id",), ("source_system",)),
    "candidate_pairs": (("left_id",), ("right_id",), ("decision",)),
    "blocking_metrics": (("pass_name",),),
    "entity_clusters": (("cluster_id",), ("source_record_id",), ("person_entity_id",)),
    "golden_records": (("golden_id",), ("cluster_id",), ("person_entity_id",)),
    "source_to_golden_crosswalk": (("source_record_id",), ("golden_id",), ("cluster_id",)),
    "review_cases": (("review_id",), ("queue_status",), ("left_id",), ("right_id",)),
}


def _create_artifact_table(table_name: str, schema: tuple[tuple[str, str], ...]) -> None:
    column_sql = ", ".join(f'"{name}" {column_type}' for name, column_type in schema)
    op.execute(
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


def _ensure_pipeline_runs_columns() -> None:
    bind = op.get_bind()
    existing_columns = {str(column["name"]) for column in inspect(bind).get_columns("pipeline_runs")}
    required_columns = {
        "run_key": "TEXT",
        "attempt_number": "INTEGER",
        "failure_detail": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            bind.exec_driver_sql(
                f"ALTER TABLE pipeline_runs ADD COLUMN {column_name} {column_type}"
            )


def upgrade() -> None:
    op.execute(
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
    _ensure_pipeline_runs_columns()
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_completed_run_key
        ON pipeline_runs (run_key)
        WHERE status = 'completed'
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_running_run_key
        ON pipeline_runs (run_key)
        WHERE status = 'running'
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_key
        ON pipeline_runs (run_key, attempt_number)
        """
    )

    for table_name, schema in ARTIFACT_TABLE_SPECS.items():
        _create_artifact_table(table_name, schema)
        for columns in ARTIFACT_INDEXES.get(table_name, ()):
            index_name = f"idx_{table_name}_{'_'.join(columns)}"
            joined_columns = ", ".join(columns)
            op.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON {table_name} (run_id, {joined_columns})
                """
            )


def downgrade() -> None:
    for table_name in (
        "review_cases",
        "source_to_golden_crosswalk",
        "golden_records",
        "entity_clusters",
        "blocking_metrics",
        "candidate_pairs",
        "normalized_source_records",
    ):
        op.execute(f"DROP TABLE IF EXISTS {table_name}")
    op.execute("DROP TABLE IF EXISTS pipeline_runs")
