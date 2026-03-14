"""Add persisted audit events for operator-sensitive actions."""

from __future__ import annotations

from alembic import op


revision = "20260314_0004"
down_revision = "20260314_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_events (
            audit_event_id TEXT PRIMARY KEY,
            occurred_at_utc TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            action TEXT NOT NULL,
            resource_type TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            run_id TEXT,
            status TEXT NOT NULL,
            details_json TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE SET NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_audit_events_run_id
        ON audit_events (run_id, occurred_at_utc)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_audit_events_action
        ON audit_events (action, occurred_at_utc)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_audit_events_status
        ON audit_events (status, occurred_at_utc)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS audit_events")
