"""Add durable run checkpoints for failed-run resume."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260314_0005"
down_revision = "20260314_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("pipeline_runs") as batch_op:
        batch_op.add_column(sa.Column("resumed_from_run_id", sa.Text(), nullable=True))

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS run_checkpoints (
            checkpoint_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            run_key TEXT NOT NULL,
            attempt_number INTEGER NOT NULL,
            stage_name TEXT NOT NULL,
            stage_order INTEGER NOT NULL,
            checkpointed_at_utc TEXT NOT NULL,
            total_duration_seconds REAL NOT NULL,
            record_counts_json TEXT NOT NULL,
            phase_metrics_json TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_run_checkpoints_run_stage
        ON run_checkpoints (run_id, stage_name)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_run_checkpoints_run_id
        ON run_checkpoints (run_id, stage_order)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_run_checkpoints_run_key
        ON run_checkpoints (run_key, attempt_number, stage_order)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS run_checkpoints")
    with op.batch_alter_table("pipeline_runs") as batch_op:
        batch_op.drop_column("resumed_from_run_id")
