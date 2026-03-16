from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import subprocess

import pytest

import etl_identity_engine.benchmark_runtime as benchmark_runtime
from etl_identity_engine.runtime_config import BenchmarkCapacityTargetConfig


def _postgresql_target(*, runtime_environment: str | None = None) -> BenchmarkCapacityTargetConfig:
    return BenchmarkCapacityTargetConfig(
        deployment_name="cluster_postgresql_baseline",
        runtime_environment=runtime_environment,
        state_store_backend="postgresql",
        max_total_duration_seconds=120.0,
        min_normalize_records_per_second=0.0,
        min_match_candidate_pairs_per_second=0.0,
    )


def _sqlite_target(*, runtime_environment: str | None = None) -> BenchmarkCapacityTargetConfig:
    return BenchmarkCapacityTargetConfig(
        deployment_name="single_host_container",
        runtime_environment=runtime_environment,
        state_store_backend="sqlite",
        max_total_duration_seconds=120.0,
        min_normalize_records_per_second=0.0,
        min_match_candidate_pairs_per_second=0.0,
    )


def test_docker_env_args_expands_environment_pairs() -> None:
    assert benchmark_runtime._docker_env_args({"POSTGRES_DB": "identity", "POSTGRES_USER": "etl"}) == [
        "-e",
        "POSTGRES_DB=identity",
        "-e",
        "POSTGRES_USER=etl",
    ]


def test_run_reports_missing_executable(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*args, **kwargs):
        raise FileNotFoundError("missing executable")

    monkeypatch.setattr(benchmark_runtime.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="Required executable 'docker' was not found"):
        benchmark_runtime._run(["docker", "run"])


def test_temporary_environment_restores_original_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETL_IDENTITY_STATE_DB", "before")
    monkeypatch.delenv("ETL_IDENTITY_SERVICE_OPERATOR_API_KEY", raising=False)

    with benchmark_runtime._temporary_environment(
        {
            "ETL_IDENTITY_STATE_DB": "after",
            "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY": "secret",
        }
    ):
        assert os.environ["ETL_IDENTITY_STATE_DB"] == "after"
        assert os.environ["ETL_IDENTITY_SERVICE_OPERATOR_API_KEY"] == "secret"

    assert os.environ["ETL_IDENTITY_STATE_DB"] == "before"
    assert "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY" not in os.environ


def test_benchmark_execution_context_rejects_mismatched_explicit_backend(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires a postgresql state store"):
        with benchmark_runtime.benchmark_execution_context(
            benchmark_root=tmp_path,
            target=_postgresql_target(),
            explicit_state_db=str(tmp_path / "state" / "pipeline_state.sqlite"),
            runtime_environment=None,
        ):
            pytest.fail("benchmark_execution_context should reject mismatched explicit backends")


def test_benchmark_execution_context_uses_explicit_state_db_with_cluster_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ETL_IDENTITY_STATE_DB", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_SERVICE_READER_API_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_SERVICE_OPERATOR_API_KEY", raising=False)

    state_db = tmp_path / "state" / "pipeline_state.sqlite"
    with benchmark_runtime.benchmark_execution_context(
        benchmark_root=tmp_path,
        target=_sqlite_target(runtime_environment="cluster"),
        explicit_state_db=str(state_db),
        runtime_environment="cluster",
    ) as context:
        assert context.state_db == str(state_db.resolve())
        assert context.state_store_backend == "sqlite"
        assert context.state_store_mode == "explicit"
        assert os.environ["ETL_IDENTITY_STATE_DB"] == context.state_db
        assert os.environ["ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY"] == "disabled"
        assert os.environ["ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY"] == "disabled"
        assert os.environ["ETL_IDENTITY_SERVICE_READER_API_KEY"] == "benchmark-reader-secret"
        assert os.environ["ETL_IDENTITY_SERVICE_OPERATOR_API_KEY"] == "benchmark-operator-secret"

    assert "ETL_IDENTITY_STATE_DB" not in os.environ


def test_benchmark_execution_context_creates_local_sqlite_by_default(tmp_path: Path) -> None:
    with benchmark_runtime.benchmark_execution_context(
        benchmark_root=tmp_path,
        target=None,
        explicit_state_db=None,
        runtime_environment=None,
    ) as context:
        state_db_path = Path(context.state_db)
        assert context.state_store_backend == "sqlite"
        assert context.state_store_mode == "local_sqlite"
        assert state_db_path.parent.exists()
        assert state_db_path.name == "pipeline_state.sqlite"


def test_benchmark_execution_context_uses_provisioned_postgresql_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def fake_provision():
        yield "postgresql://etl_identity:benchmark-password@127.0.0.1:55432/identity_state", (
            "ephemeral_postgresql_container"
        )

    monkeypatch.setattr(benchmark_runtime, "_provision_postgresql_state_store", fake_provision)
    monkeypatch.delenv("ETL_IDENTITY_STATE_DB", raising=False)

    with benchmark_runtime.benchmark_execution_context(
        benchmark_root=tmp_path,
        target=_postgresql_target(runtime_environment="cluster"),
        explicit_state_db=None,
        runtime_environment="cluster",
    ) as context:
        assert context.state_store_backend == "postgresql"
        assert context.state_store_mode == "ephemeral_postgresql_container"
        assert os.environ["ETL_IDENTITY_STATE_DB"] == context.state_db

    assert "ETL_IDENTITY_STATE_DB" not in os.environ


def test_provision_postgresql_state_store_runs_docker_and_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    run_calls: list[tuple[list[str], bool]] = []
    wait_calls: list[str] = []

    monkeypatch.setattr(benchmark_runtime, "_find_free_tcp_port", lambda: 55432)
    monkeypatch.setattr(benchmark_runtime.time, "time", lambda: 1700000000)
    monkeypatch.setattr(benchmark_runtime, "_wait_for_postgresql", lambda state_db: wait_calls.append(state_db))

    def fake_run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        run_calls.append((command, check))
        return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(benchmark_runtime, "_run", fake_run)

    with benchmark_runtime._provision_postgresql_state_store() as (state_db, state_store_mode):
        assert state_db == "postgresql://etl_identity:benchmark-password@127.0.0.1:55432/identity_state"
        assert state_store_mode == "ephemeral_postgresql_container"

    assert wait_calls == ["postgresql://etl_identity:benchmark-password@127.0.0.1:55432/identity_state"]
    assert run_calls[0] == (
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            "etl-identity-benchmark-postgres-1700000000-55432",
            "-p",
            "55432:5432",
            "-e",
            "POSTGRES_DB=identity_state",
            "-e",
            "POSTGRES_USER=etl_identity",
            "-e",
            "POSTGRES_PASSWORD=benchmark-password",
            "postgres:16-alpine",
        ],
        True,
    )
    assert run_calls[-1] == (
        ["docker", "rm", "-f", "etl-identity-benchmark-postgres-1700000000-55432"],
        False,
    )
