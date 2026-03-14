"""Alembic-backed migration helpers for the SQLite pipeline store."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine


def _sqlite_url(db_path: Path) -> str:
    resolved = Path(db_path).resolve()
    return f"sqlite:///{resolved.as_posix()}"


def _build_alembic_config(db_path: Path) -> Config:
    migrations_dir = Path(__file__).resolve().with_name("migrations")
    config = Config()
    config.set_main_option("script_location", str(migrations_dir))
    config.set_main_option("sqlalchemy.url", _sqlite_url(db_path))
    return config


def head_revision() -> str:
    config = _build_alembic_config(Path("pipeline_state.sqlite"))
    script_directory = ScriptDirectory.from_config(config)
    head = script_directory.get_current_head()
    if head is None:
        raise RuntimeError("Alembic head revision is not defined")
    return str(head)


def upgrade_sqlite_store(db_path: Path, revision: str = "head") -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    command.upgrade(_build_alembic_config(db_path), revision)


def current_sqlite_store_revision(db_path: Path) -> str | None:
    db_path = Path(db_path)
    if not db_path.exists():
        return None

    engine = create_engine(_sqlite_url(db_path))
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(connection)
            revision = context.get_current_revision()
    finally:
        engine.dispose()
    return None if revision is None else str(revision)
