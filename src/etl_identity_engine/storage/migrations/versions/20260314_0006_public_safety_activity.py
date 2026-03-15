"""Persist public-safety activity artifacts alongside completed runs."""

from __future__ import annotations

from alembic import op

from etl_identity_engine.output_contracts import (
    PUBLIC_SAFETY_GOLDEN_ACTIVITY_HEADERS,
    PUBLIC_SAFETY_INCIDENT_IDENTITY_HEADERS,
)


revision = "20260314_0006"
down_revision = "20260314_0005"
branch_labels = None
depends_on = None


ARTIFACT_TABLE_SPECS: dict[str, tuple[tuple[str, str], ...]] = {
    "public_safety_incident_identity": tuple(
        (column, "TEXT") for column in PUBLIC_SAFETY_INCIDENT_IDENTITY_HEADERS
    ),
    "public_safety_golden_activity": tuple(
        (column, "TEXT") for column in PUBLIC_SAFETY_GOLDEN_ACTIVITY_HEADERS
    ),
}

ARTIFACT_INDEXES: dict[str, tuple[tuple[str, ...], ...]] = {
    "public_safety_incident_identity": (
        ("incident_id",),
        ("golden_id",),
        ("cluster_id",),
        ("source_record_id",),
        ("person_entity_id",),
        ("incident_source_system",),
    ),
    "public_safety_golden_activity": (
        ("golden_id",),
        ("cluster_id",),
        ("person_entity_id",),
    ),
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


def upgrade() -> None:
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
    op.execute("DROP TABLE IF EXISTS public_safety_golden_activity")
    op.execute("DROP TABLE IF EXISTS public_safety_incident_identity")
