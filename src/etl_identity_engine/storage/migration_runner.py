"""Alembic-backed migration helpers for persisted SQL state stores."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext
from etl_identity_engine.storage.state_store_target import (
    create_state_store_engine,
    resolve_state_store_target,
)


def _build_alembic_config(state_db: str | Path) -> Config:
    target = resolve_state_store_target(state_db)
    migrations_dir = Path(__file__).resolve().with_name("migrations")
    config = Config()
    config.set_main_option("script_location", str(migrations_dir))
    config.set_main_option("sqlalchemy.url", target.sqlalchemy_url)
    return config


def head_revision() -> str:
    config = _build_alembic_config(Path("pipeline_state.sqlite"))
    script_directory = ScriptDirectory.from_config(config)
    head = script_directory.get_current_head()
    if head is None:
        raise RuntimeError("Alembic head revision is not defined")
    return str(head)


def upgrade_state_store(state_db: str | Path, revision: str = "head") -> None:
    target = resolve_state_store_target(state_db)
    if target.file_path is not None:
        target.file_path.parent.mkdir(parents=True, exist_ok=True)
    command.upgrade(_build_alembic_config(target.raw_value), revision)


def current_state_store_revision(state_db: str | Path) -> str | None:
    target = resolve_state_store_target(state_db)
    if target.file_path is not None and not target.file_path.exists():
        return None

    engine = create_state_store_engine(target)
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(connection)
            revision = context.get_current_revision()
    finally:
        engine.dispose()
    return None if revision is None else str(revision)


def upgrade_sqlite_store(db_path: Path, revision: str = "head") -> None:
    upgrade_state_store(db_path, revision=revision)


def current_sqlite_store_revision(db_path: Path) -> str | None:
    return current_state_store_revision(db_path)
