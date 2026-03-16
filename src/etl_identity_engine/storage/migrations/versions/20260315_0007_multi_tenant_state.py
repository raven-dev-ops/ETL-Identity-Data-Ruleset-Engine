"""Add tenant boundaries to persisted pipeline state."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "20260315_0007"
down_revision = "20260314_0006"
branch_labels = None
depends_on = None


DEFAULT_TENANT_ID = "default"
TENANT_SCOPED_RUN_TABLES = (
    "run_checkpoints",
    "normalized_source_records",
    "candidate_pairs",
    "blocking_metrics",
    "entity_clusters",
    "golden_records",
    "source_to_golden_crosswalk",
    "review_cases",
    "public_safety_incident_identity",
    "public_safety_golden_activity",
)


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    existing_columns = {str(column["name"]) for column in inspect(bind).get_columns(table_name)}
    return column_name in existing_columns


def _add_tenant_column(table_name: str) -> None:
    if _has_column(table_name, "tenant_id"):
        return
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.add_column(
            sa.Column(
                "tenant_id",
                sa.Text(),
                nullable=False,
                server_default=sa.text(f"'{DEFAULT_TENANT_ID}'"),
            )
        )


def _drop_tenant_column(table_name: str) -> None:
    if not _has_column(table_name, "tenant_id"):
        return
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.drop_column("tenant_id")


def upgrade() -> None:
    _add_tenant_column("pipeline_runs")
    _add_tenant_column("export_job_runs")
    _add_tenant_column("audit_events")
    for table_name in TENANT_SCOPED_RUN_TABLES:
        _add_tenant_column(table_name)

    op.execute(
        f"""
        UPDATE pipeline_runs
        SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '{DEFAULT_TENANT_ID}')
        """
    )
    op.execute(
        f"""
        UPDATE export_job_runs
        SET tenant_id = COALESCE(
            (
                SELECT pipeline_runs.tenant_id
                FROM pipeline_runs
                WHERE pipeline_runs.run_id = export_job_runs.source_run_id
            ),
            NULLIF(export_job_runs.tenant_id, ''),
            '{DEFAULT_TENANT_ID}'
        )
        """
    )
    op.execute(
        f"""
        UPDATE audit_events
        SET tenant_id = COALESCE(
            (
                SELECT pipeline_runs.tenant_id
                FROM pipeline_runs
                WHERE pipeline_runs.run_id = audit_events.run_id
            ),
            NULLIF(audit_events.tenant_id, ''),
            '{DEFAULT_TENANT_ID}'
        )
        """
    )
    for table_name in TENANT_SCOPED_RUN_TABLES:
        op.execute(
            f"""
            UPDATE {table_name}
            SET tenant_id = COALESCE(
                (
                    SELECT pipeline_runs.tenant_id
                    FROM pipeline_runs
                    WHERE pipeline_runs.run_id = {table_name}.run_id
                ),
                NULLIF({table_name}.tenant_id, ''),
                '{DEFAULT_TENANT_ID}'
            )
            """
        )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pipeline_runs_tenant_status_finished
        ON pipeline_runs (tenant_id, status, finished_at_utc, started_at_utc)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pipeline_runs_tenant_run_key_attempt
        ON pipeline_runs (tenant_id, run_key, attempt_number)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_export_job_runs_tenant_status_started
        ON export_job_runs (tenant_id, status, started_at_utc)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_export_job_runs_tenant_export_key_attempt
        ON export_job_runs (tenant_id, export_key, attempt_number)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_occurred
        ON audit_events (tenant_id, occurred_at_utc)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_action
        ON audit_events (tenant_id, action, occurred_at_utc)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_run_checkpoints_tenant_run_key
        ON run_checkpoints (tenant_id, run_key, attempt_number, stage_order)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_review_cases_tenant_run_queue_status
        ON review_cases (tenant_id, run_id, queue_status)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_review_cases_tenant_run_queue_status")
    op.execute("DROP INDEX IF EXISTS idx_run_checkpoints_tenant_run_key")
    op.execute("DROP INDEX IF EXISTS idx_audit_events_tenant_action")
    op.execute("DROP INDEX IF EXISTS idx_audit_events_tenant_occurred")
    op.execute("DROP INDEX IF EXISTS idx_export_job_runs_tenant_export_key_attempt")
    op.execute("DROP INDEX IF EXISTS idx_export_job_runs_tenant_status_started")
    op.execute("DROP INDEX IF EXISTS idx_pipeline_runs_tenant_run_key_attempt")
    op.execute("DROP INDEX IF EXISTS idx_pipeline_runs_tenant_status_finished")

    for table_name in reversed(TENANT_SCOPED_RUN_TABLES):
        _drop_tenant_column(table_name)
    _drop_tenant_column("audit_events")
    _drop_tenant_column("export_job_runs")
    _drop_tenant_column("pipeline_runs")
