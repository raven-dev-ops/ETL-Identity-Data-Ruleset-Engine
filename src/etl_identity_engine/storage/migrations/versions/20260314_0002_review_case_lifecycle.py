"""Add persisted manual-review workflow fields."""

from __future__ import annotations

from alembic import op


revision = "20260314_0002"
down_revision = "20260314_0001"
branch_labels = None
depends_on = None


def _ensure_column(column_name: str, column_type: str) -> None:
    bind = op.get_bind()
    rows = bind.exec_driver_sql("PRAGMA table_info(review_cases)").fetchall()
    existing_columns = {str(row[1]) for row in rows}
    if column_name not in existing_columns:
        bind.exec_driver_sql(f"ALTER TABLE review_cases ADD COLUMN {column_name} {column_type}")


def upgrade() -> None:
    for column_name in (
        ("assigned_to", "TEXT"),
        ("operator_notes", "TEXT"),
        ("created_at_utc", "TEXT"),
        ("updated_at_utc", "TEXT"),
        ("resolved_at_utc", "TEXT"),
    ):
        _ensure_column(*column_name)

    op.execute(
        """
        UPDATE review_cases
        SET queue_status = COALESCE(NULLIF(queue_status, ''), 'pending'),
            assigned_to = COALESCE(assigned_to, ''),
            operator_notes = COALESCE(operator_notes, '')
        """
    )
    op.execute(
        """
        UPDATE review_cases
        SET created_at_utc = COALESCE(
                NULLIF(created_at_utc, ''),
                (
                    SELECT pipeline_runs.finished_at_utc
                    FROM pipeline_runs
                    WHERE pipeline_runs.run_id = review_cases.run_id
                ),
                ''
            )
        """
    )
    op.execute(
        """
        UPDATE review_cases
        SET updated_at_utc = COALESCE(NULLIF(updated_at_utc, ''), created_at_utc, '')
        """
    )
    op.execute(
        """
        UPDATE review_cases
        SET resolved_at_utc = CASE
            WHEN queue_status IN ('approved', 'rejected')
                THEN COALESCE(NULLIF(resolved_at_utc, ''), updated_at_utc, '')
            ELSE ''
        END
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_review_cases_assigned_to
        ON review_cases (run_id, assigned_to)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_review_cases_assigned_to")
