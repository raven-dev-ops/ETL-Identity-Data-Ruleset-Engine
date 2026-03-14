from __future__ import annotations

import sqlite3
from pathlib import Path

from etl_identity_engine.cli import main
from etl_identity_engine.storage.migration_runner import (
    current_sqlite_store_revision,
    head_revision,
)
from etl_identity_engine.storage.sqlite_store import PIPELINE_STATE_TABLES, bootstrap_sqlite_store


def test_bootstrap_sqlite_store_applies_alembic_head_revision(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"

    bootstrap_sqlite_store(db_path)

    assert current_sqlite_store_revision(db_path) == head_revision()
    with sqlite3.connect(db_path) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert set(PIPELINE_STATE_TABLES) <= table_names


def test_state_db_upgrade_command_can_use_runtime_environment_defaults(
    tmp_path: Path,
    capsys,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    runtime_config.write_text(
        """
default_environment: dev
environments:
  dev:
    config_dir: ./config
    state_db: ./state/dev.sqlite
""".strip()
        + "\n",
        encoding="utf-8",
    )

    assert (
        main(
            [
                "state-db-upgrade",
                "--environment",
                "dev",
                "--runtime-config",
                str(runtime_config),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "state-db-current",
                "--environment",
                "dev",
                "--runtime-config",
                str(runtime_config),
            ]
        )
        == 0
    )

    captured = capsys.readouterr().out
    assert "state db upgraded:" in captured
    assert f"head={head_revision()}" in captured
    assert current_sqlite_store_revision(tmp_path / "state" / "dev.sqlite") == head_revision()
