"""Runtime helpers for benchmark execution targets."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
import socket
import subprocess
import time
from typing import Iterator

from sqlalchemy import text

from etl_identity_engine.runtime_config import BenchmarkCapacityTargetConfig
from etl_identity_engine.storage.state_store_target import (
    create_state_store_engine,
    resolve_state_store_target,
    state_store_display_name,
)


@dataclass(frozen=True)
class BenchmarkExecutionContext:
    state_db: str
    state_store_backend: str
    runtime_environment: str | None
    state_db_display_name: str
    state_store_mode: str


def _docker_env_args(env_map: dict[str, str]) -> list[str]:
    args: list[str] = []
    for key, value in env_map.items():
        args.extend(["-e", f"{key}={value}"])
    return args


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=check,
            text=True,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Required executable {command[0]!r} was not found while preparing the benchmark runtime"
        ) from exc


def _wait_for_postgresql(state_db: str, *, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    target = resolve_state_store_target(state_db)
    while time.time() < deadline:
        engine = create_state_store_engine(target)
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except Exception:
            time.sleep(1)
        finally:
            engine.dispose()
    raise RuntimeError("Timed out waiting for benchmark PostgreSQL readiness")


@contextmanager
def _temporary_environment(overrides: dict[str, str]) -> Iterator[None]:
    original: dict[str, str | None] = {key: os.environ.get(key) for key in overrides}
    os.environ.update(overrides)
    try:
        yield
    finally:
        for key, original_value in original.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value


@contextmanager
def _provision_postgresql_state_store() -> Iterator[tuple[str, str]]:
    host_port = _find_free_tcp_port()
    container_name = f"etl-identity-benchmark-postgres-{int(time.time())}-{host_port}"
    database_name = "identity_state"
    username = "etl_identity"
    password = "benchmark-password"
    state_db = (
        f"postgresql://{username}:{password}@127.0.0.1:{host_port}/{database_name}"
    )
    postgres_env = {
        "POSTGRES_DB": database_name,
        "POSTGRES_USER": username,
        "POSTGRES_PASSWORD": password,
    }
    try:
        _run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                container_name,
                "-p",
                f"{host_port}:5432",
                *_docker_env_args(postgres_env),
                "postgres:16-alpine",
            ]
        )
        _wait_for_postgresql(state_db)
        yield state_db, "ephemeral_postgresql_container"
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Failed to provision benchmark PostgreSQL runtime:\n{exc.stdout}{exc.stderr}"
        ) from exc
    finally:
        _run(["docker", "rm", "-f", container_name], check=False)


def _runtime_environment_overrides(
    runtime_environment: str | None,
    *,
    state_db: str,
) -> dict[str, str]:
    if runtime_environment != "cluster":
        return {}
    return {
        "ETL_IDENTITY_STATE_DB": state_db,
        "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY": "disabled",
        "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY": "disabled",
        "ETL_IDENTITY_SERVICE_READER_API_KEY": "benchmark-reader-secret",
        "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY": "benchmark-operator-secret",
    }


@contextmanager
def benchmark_execution_context(
    *,
    benchmark_root: Path,
    target: BenchmarkCapacityTargetConfig | None,
    explicit_state_db: str | None,
    runtime_environment: str | None,
) -> Iterator[BenchmarkExecutionContext]:
    expected_backend = "sqlite" if target is None else target.state_store_backend
    if explicit_state_db is not None:
        resolved = resolve_state_store_target(explicit_state_db)
        if resolved.backend != expected_backend:
            raise ValueError(
                f"Benchmark deployment target {target.deployment_name if target else ''!r} requires "
                f"a {expected_backend} state store, but --state-db resolved to {resolved.backend}"
            )
        env_overrides = _runtime_environment_overrides(runtime_environment, state_db=resolved.raw_value)
        with _temporary_environment(env_overrides):
            yield BenchmarkExecutionContext(
                state_db=resolved.raw_value,
                state_store_backend=resolved.backend,
                runtime_environment=runtime_environment,
                state_db_display_name=state_store_display_name(resolved.raw_value),
                state_store_mode="explicit",
            )
        return

    if expected_backend == "postgresql":
        with _provision_postgresql_state_store() as (state_db, state_store_mode):
            env_overrides = _runtime_environment_overrides(runtime_environment, state_db=state_db)
            with _temporary_environment(env_overrides):
                yield BenchmarkExecutionContext(
                    state_db=state_db,
                    state_store_backend="postgresql",
                    runtime_environment=runtime_environment,
                    state_db_display_name=state_store_display_name(state_db),
                    state_store_mode=state_store_mode,
                )
        return

    sqlite_path = benchmark_root / "state" / "pipeline_state.sqlite"
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    state_db = str(sqlite_path)
    env_overrides = _runtime_environment_overrides(runtime_environment, state_db=state_db)
    with _temporary_environment(env_overrides):
        yield BenchmarkExecutionContext(
            state_db=state_db,
            state_store_backend="sqlite",
            runtime_environment=runtime_environment,
            state_db_display_name=state_store_display_name(state_db),
            state_store_mode="local_sqlite",
        )
