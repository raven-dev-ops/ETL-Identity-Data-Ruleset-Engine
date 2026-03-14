"""State-store target parsing and SQLAlchemy engine creation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL, make_url


SUPPORTED_STATE_STORE_BACKENDS = frozenset({"sqlite", "postgresql"})
SQLALCHEMY_URL_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9+.+-]*://")


@dataclass(frozen=True)
class StateStoreTarget:
    raw_value: str
    sqlalchemy_url: str
    backend: str
    display_name: str
    file_path: Path | None


def _is_sqlalchemy_url(value: str) -> bool:
    return bool(SQLALCHEMY_URL_PATTERN.match(value))


def is_state_store_url(value: str) -> bool:
    return _is_sqlalchemy_url(value.strip())


def _resolve_local_sqlite_target(raw_value: str) -> StateStoreTarget:
    if raw_value == ":memory:":
        return StateStoreTarget(
            raw_value=raw_value,
            sqlalchemy_url="sqlite:///:memory:",
            backend="sqlite",
            display_name="sqlite:///:memory:",
            file_path=None,
        )

    file_path = Path(raw_value).expanduser().resolve()
    return StateStoreTarget(
        raw_value=str(file_path),
        sqlalchemy_url=f"sqlite:///{file_path.as_posix()}",
        backend="sqlite",
        display_name=str(file_path),
        file_path=file_path,
    )


def _normalize_postgresql_driver(url: URL) -> URL:
    if url.drivername == "postgresql":
        return url.set(drivername="postgresql+psycopg")
    return url


def _resolve_sqlalchemy_target(raw_value: str) -> StateStoreTarget:
    url = make_url(raw_value)
    backend = url.get_backend_name()
    if backend not in SUPPORTED_STATE_STORE_BACKENDS:
        raise ValueError(
            "Unsupported state store backend "
            f"{backend!r}; expected one of {sorted(SUPPORTED_STATE_STORE_BACKENDS)}"
        )

    if backend == "postgresql":
        normalized_url = _normalize_postgresql_driver(url)
        normalized_raw_value = normalized_url.render_as_string(hide_password=False)
        return StateStoreTarget(
            raw_value=normalized_raw_value,
            sqlalchemy_url=normalized_raw_value,
            backend=backend,
            display_name=normalized_url.render_as_string(hide_password=True),
            file_path=None,
        )

    database = url.database or ""
    if database == ":memory:":
        normalized_url = url.set(database=":memory:")
        normalized_raw_value = normalized_url.render_as_string(hide_password=False)
        return StateStoreTarget(
            raw_value=normalized_raw_value,
            sqlalchemy_url=normalized_raw_value,
            backend=backend,
            display_name=normalized_url.render_as_string(hide_password=True),
            file_path=None,
        )

    sqlite_path = Path(database).expanduser()
    if not sqlite_path.is_absolute():
        sqlite_path = sqlite_path.resolve()
    normalized_url = url.set(database=sqlite_path.as_posix())
    normalized_raw_value = normalized_url.render_as_string(hide_password=False)
    return StateStoreTarget(
        raw_value=normalized_raw_value,
        sqlalchemy_url=normalized_raw_value,
        backend=backend,
        display_name=str(sqlite_path),
        file_path=sqlite_path,
    )


def resolve_state_store_target(state_db: str | Path) -> StateStoreTarget:
    raw_value = str(state_db).strip()
    if not raw_value:
        raise ValueError("State store target must be a non-empty path or SQLAlchemy URL")
    if _is_sqlalchemy_url(raw_value):
        return _resolve_sqlalchemy_target(raw_value)
    return _resolve_local_sqlite_target(raw_value)


def create_state_store_engine(state_db: str | Path | StateStoreTarget) -> Engine:
    target = state_db if isinstance(state_db, StateStoreTarget) else resolve_state_store_target(state_db)
    if target.file_path is not None:
        target.file_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(target.sqlalchemy_url, future=True)
    if target.backend == "sqlite":
        event.listen(engine, "connect", _set_sqlite_foreign_keys)
    return engine


def state_store_display_name(state_db: str | Path | StateStoreTarget) -> str:
    target = state_db if isinstance(state_db, StateStoreTarget) else resolve_state_store_target(state_db)
    return target.display_name


def state_store_reference_name(state_db: str | Path | StateStoreTarget) -> str:
    target = state_db if isinstance(state_db, StateStoreTarget) else resolve_state_store_target(state_db)
    if target.file_path is not None:
        return target.file_path.name
    return target.display_name


def _set_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys = ON")
    finally:
        cursor.close()
