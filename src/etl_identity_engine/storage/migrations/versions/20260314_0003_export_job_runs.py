"""Add tracked downstream export-job runs."""

from __future__ import annotations

from alembic import op


revision = "20260314_0003"
down_revision = "20260314_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS export_job_runs (
            export_run_id TEXT PRIMARY KEY,
            export_key TEXT NOT NULL,
            attempt_number INTEGER NOT NULL,
            job_name TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            contract_name TEXT NOT NULL,
            contract_version TEXT NOT NULL,
            output_root TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT NOT NULL,
            snapshot_dir TEXT NOT NULL,
            current_pointer_path TEXT NOT NULL,
            row_counts_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            failure_detail TEXT
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_export_job_runs_completed_export_key
        ON export_job_runs (export_key)
        WHERE status = 'completed'
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_export_job_runs_running_export_key
        ON export_job_runs (export_key)
        WHERE status = 'running'
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_export_job_runs_job_name
        ON export_job_runs (job_name, started_at_utc)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_export_job_runs_source_run_id
        ON export_job_runs (source_run_id, started_at_utc)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS export_job_runs")
