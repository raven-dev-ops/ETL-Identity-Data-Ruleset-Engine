from __future__ import annotations

from pathlib import Path

import pytest

from etl_identity_engine.runtime_config import load_runtime_environment
from etl_identity_engine.storage import migration_runner
from etl_identity_engine.storage.state_store_target import (
    resolve_state_store_target,
    state_store_display_name,
    state_store_reference_name,
)


def test_resolve_state_store_target_normalizes_postgresql_driver_and_hides_password() -> None:
    target = resolve_state_store_target(
        "postgresql://etl_user:supersecret@db.internal:5432/identity_state"
    )

    assert target.backend == "postgresql"
    assert target.raw_value == "postgresql+psycopg://etl_user:supersecret@db.internal:5432/identity_state"
    assert target.sqlalchemy_url == target.raw_value
    assert target.display_name == "postgresql+psycopg://etl_user:***@db.internal:5432/identity_state"
    assert target.file_path is None


def test_state_store_reference_name_uses_filename_for_file_backed_sqlite(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"

    assert state_store_display_name(db_path) == str(db_path.resolve())
    assert state_store_reference_name(db_path) == "pipeline_state.sqlite"


def test_load_runtime_environment_allows_postgresql_state_store_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    runtime_config.write_text(
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${ETL_IDENTITY_STATE_DB}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "ETL_IDENTITY_STATE_DB",
        "postgresql://etl_user:supersecret@db.internal:5432/identity_state",
    )

    environment = load_runtime_environment("prod", runtime_config)

    assert environment.state_db == "postgresql+psycopg://etl_user:supersecret@db.internal:5432/identity_state"


def test_upgrade_state_store_accepts_postgresql_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    def fake_upgrade(config, revision: str) -> None:
        captured["sqlalchemy_url"] = config.get_main_option("sqlalchemy.url")
        captured["revision"] = revision

    monkeypatch.setattr(migration_runner.command, "upgrade", fake_upgrade)

    migration_runner.upgrade_state_store(
        "postgresql://etl_user:supersecret@db.internal:5432/identity_state",
        revision="head",
    )

    assert captured == {
        "sqlalchemy_url": "postgresql+psycopg://etl_user:supersecret@db.internal:5432/identity_state",
        "revision": "head",
    }


def test_current_state_store_revision_uses_postgresql_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeContext:
        def get_current_revision(self) -> str:
            return "20260314_0004"

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeEngine:
        def connect(self):
            captured["connected"] = True
            return FakeConnection()

        def dispose(self) -> None:
            captured["disposed"] = True

    def fake_create_state_store_engine(state_db):
        captured["state_db"] = state_db
        return FakeEngine()

    def fake_configure(connection):
        captured["connection"] = connection
        return FakeContext()

    monkeypatch.setattr(migration_runner, "create_state_store_engine", fake_create_state_store_engine)
    monkeypatch.setattr(migration_runner.MigrationContext, "configure", staticmethod(fake_configure))

    revision = migration_runner.current_state_store_revision(
        "postgresql://etl_user:supersecret@db.internal:5432/identity_state"
    )

    assert revision == "20260314_0004"
    assert captured["connected"] is True
    assert captured["disposed"] is True
