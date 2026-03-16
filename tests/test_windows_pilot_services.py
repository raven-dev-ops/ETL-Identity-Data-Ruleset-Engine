from __future__ import annotations

import json
from pathlib import Path

import pytest

import etl_identity_engine.windows_pilot_services as windows_pilot_services
from etl_identity_engine.windows_pilot_services import (
    WindowsPilotServiceDefinition,
    WindowsPilotServiceStatus,
    load_windows_pilot_service_definitions,
    manage_windows_pilot_services,
)


def _expected_service_command(bundle_root: Path, expected_args: list[str]) -> list[str]:
    resolved_args: list[str] = []
    for arg in expected_args:
        if arg.endswith(".py"):
            resolved_args.append(str(bundle_root.joinpath(*arg.split("\\"))))
        elif arg.startswith("runtime\\"):
            resolved_args.append(str(bundle_root.joinpath(*arg.split("\\"))))
        else:
            resolved_args.append(arg)
    return resolved_args


def _write_bootstrap_bundle(root: Path) -> None:
    (root / "runtime").mkdir(parents=True, exist_ok=True)
    (root / "pilot_manifest.json").write_text(
        json.dumps({"pilot_name": "public-safety-regressions", "version": "1.0.0"}) + "\n",
        encoding="utf-8",
    )
    (root / "runtime" / "pilot_bootstrap.json").write_text(
        json.dumps(
            {
                "pilot_name": "public-safety-regressions",
                "demo_host": "127.0.0.1",
                "demo_port": 8000,
                "service_host": "127.0.0.1",
                "service_port": 8010,
                "windows_service_startup": "manual",
            }
        )
        + "\n",
        encoding="utf-8",
    )


def test_load_windows_pilot_service_definitions_uses_bootstrap_config(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    _write_bootstrap_bundle(bundle_root)

    definitions = load_windows_pilot_service_definitions(bundle_root)

    assert set(definitions) == {"demo_shell", "service_api"}
    assert definitions["demo_shell"].display_name == "ETL Identity Pilot Demo Shell (public-safety-regressions)"
    assert definitions["demo_shell"].port == 8000
    assert definitions["service_api"].display_name == "ETL Identity Pilot Service API (public-safety-regressions)"
    assert definitions["service_api"].port == 8010


def test_manage_windows_pilot_services_sequences_actions(tmp_path: Path, monkeypatch) -> None:
    bundle_root = tmp_path / "bundle"
    _write_bootstrap_bundle(bundle_root)

    install_calls: list[tuple[str, str | None]] = []
    start_calls: list[str] = []
    status_calls: list[str] = []

    def fake_install(*, bundle_root: Path, service_kind: str, startup: str | None):
        install_calls.append((service_kind, startup))
        return WindowsPilotServiceDefinition(
            kind=service_kind,
            service_name=f"svc-{service_kind}",
            display_name=f"display-{service_kind}",
            description=f"description-{service_kind}",
            python_class=f"class-{service_kind}",
            host="127.0.0.1",
            port=8000 if service_kind == "demo_shell" else 8010,
            startup="auto" if startup == "auto" else "manual",
        )

    def fake_start(service_kind: str, *, bundle_root: Path):
        start_calls.append(service_kind)

    def fake_status(service_kind: str, *, bundle_root: Path):
        status_calls.append(service_kind)
        return WindowsPilotServiceStatus(
            kind=service_kind,
            service_name=f"svc-{service_kind}",
            display_name=f"display-{service_kind}",
            installed=True,
            status_code=4,
            status="running",
        )

    monkeypatch.setattr(
        "etl_identity_engine.windows_pilot_services.install_windows_pilot_service",
        fake_install,
    )
    monkeypatch.setattr(
        "etl_identity_engine.windows_pilot_services.start_windows_pilot_service",
        fake_start,
    )
    monkeypatch.setattr(
        "etl_identity_engine.windows_pilot_services.query_windows_pilot_service_status",
        fake_status,
    )

    summary = manage_windows_pilot_services(
        bundle_root=bundle_root,
        action="install-and-start",
        service_kind="all",
        startup="auto",
    )

    assert install_calls == [("demo_shell", "auto"), ("service_api", "auto")]
    assert start_calls == ["demo_shell", "service_api"]
    assert status_calls == ["demo_shell", "service_api"]
    assert summary["action"] == "install-and-start"
    assert len(summary["results"]) == 4


def test_read_env_file_ignores_comments_and_invalid_lines(tmp_path: Path) -> None:
    env_path = tmp_path / "runtime.env"
    env_path.write_text(
        """
# comment
ETL_IDENTITY_STATE_DB=state.sqlite
INVALID_LINE
ETL_IDENTITY_SERVICE_OPERATOR_API_KEY = secret
""".strip()
        + "\n",
        encoding="utf-8",
    )

    assert windows_pilot_services._read_env_file(env_path) == {
        "ETL_IDENTITY_STATE_DB": "state.sqlite",
        "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY": "secret",
    }


def test_service_startup_to_value_maps_manual_and_auto(monkeypatch) -> None:
    fake_win32service = type(
        "FakeWin32Service",
        (),
        {"SERVICE_DEMAND_START": 3, "SERVICE_AUTO_START": 2},
    )
    monkeypatch.setattr(
        windows_pilot_services,
        "_win32_modules",
        lambda: (None, None, fake_win32service, None),
    )

    assert windows_pilot_services._service_startup_to_value("manual") == 3
    assert windows_pilot_services._service_startup_to_value("auto") == 2

    with pytest.raises(ValueError, match="Service startup must be 'manual' or 'auto'"):
        windows_pilot_services._service_startup_to_value("delayed")


def test_query_windows_pilot_service_status_returns_not_installed(tmp_path: Path, monkeypatch) -> None:
    definition = WindowsPilotServiceDefinition(
        kind="demo_shell",
        service_name="svc-demo_shell",
        display_name="display-demo_shell",
        description="description-demo_shell",
        python_class="class-demo_shell",
        host="127.0.0.1",
        port=8000,
        startup="manual",
    )

    monkeypatch.setattr(windows_pilot_services, "_require_windows_host", lambda: None)
    monkeypatch.setattr(
        windows_pilot_services,
        "load_windows_pilot_service_definitions",
        lambda bundle_root: {"demo_shell": definition},
    )
    monkeypatch.setattr(windows_pilot_services, "_service_exists", lambda service_name: False)

    status = windows_pilot_services.query_windows_pilot_service_status("demo_shell", bundle_root=tmp_path)

    assert status == WindowsPilotServiceStatus(
        kind="demo_shell",
        service_name="svc-demo_shell",
        display_name="display-demo_shell",
        installed=False,
        status_code=None,
        status="not_installed",
    )


def test_service_exists_returns_false_for_missing_service(monkeypatch) -> None:
    class MissingServiceError(Exception):
        def __init__(self) -> None:
            super().__init__(
                windows_pilot_services.WINDOWS_ERROR_SERVICE_DOES_NOT_EXIST,
                "QueryServiceStatus",
                "The specified service does not exist as an installed service.",
            )

    class FakeWin32ServiceUtil:
        @staticmethod
        def QueryServiceStatus(service_name: str):
            raise MissingServiceError

    monkeypatch.setattr(
        windows_pilot_services,
        "_win32_modules",
        lambda: (None, None, None, FakeWin32ServiceUtil),
    )

    assert windows_pilot_services._service_exists("svc-demo_shell") is False


def test_service_exists_propagates_non_missing_service_errors(monkeypatch) -> None:
    class AccessDeniedError(Exception):
        def __init__(self) -> None:
            super().__init__(5, "QueryServiceStatus", "Access is denied.")

    class FakeWin32ServiceUtil:
        @staticmethod
        def QueryServiceStatus(service_name: str):
            raise AccessDeniedError

    monkeypatch.setattr(
        windows_pilot_services,
        "_win32_modules",
        lambda: (None, None, None, FakeWin32ServiceUtil),
    )

    with pytest.raises(AccessDeniedError):
        windows_pilot_services._service_exists("svc-demo_shell")


@pytest.mark.parametrize(
    ("service_kind", "expected_args"),
    [
        (
            "demo_shell",
            [
                "tools\\rebuild_demo_shell.py",
                "--state-db",
                "state.sqlite",
                "--run-id",
                "RUN-1",
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
            ],
        ),
        (
            "service_api",
            [
                "-m",
                "etl_identity_engine.cli",
                "serve-api",
                "--environment",
                "container",
                "--runtime-config",
                "runtime\\config\\runtime_environments.yml",
                "--state-db",
                "state.sqlite",
                "--host",
                "127.0.0.1",
                "--port",
                "8010",
            ],
        ),
    ],
)
def test_service_subprocess_command_builds_expected_commands(
    tmp_path: Path,
    monkeypatch,
    service_kind: str,
    expected_args: list[str],
) -> None:
    bundle_root = tmp_path / "bundle"
    python_executable = bundle_root / ".venv" / "Scripts" / "python.exe"
    python_executable.parent.mkdir(parents=True, exist_ok=True)
    python_executable.write_text("", encoding="utf-8")

    monkeypatch.setattr(windows_pilot_services, "_service_bundle_root", lambda service_name: bundle_root)
    monkeypatch.setattr(
        windows_pilot_services,
        "_service_bootstrap_config",
        lambda service_name: {
            "state_db": "state.sqlite",
            "run_id": "RUN-1",
            "demo_host": "127.0.0.1",
            "demo_port": 8000,
            "service_host": "127.0.0.1",
            "service_port": 8010,
        },
    )
    monkeypatch.setattr(windows_pilot_services, "_service_runtime_env", lambda service_name: {"EXTRA": "1"})
    monkeypatch.setattr(
        windows_pilot_services,
        "_service_runtime_option",
        lambda service_name, option_name, default=None: default,
    )

    command, cwd, env = windows_pilot_services._service_subprocess_command("svc-name", service_kind)

    assert command[0] == str(python_executable)
    assert command[1:] == _expected_service_command(bundle_root, expected_args)
    assert cwd == bundle_root
    assert env["EXTRA"] == "1"
    assert env["PYTHONUNBUFFERED"] == "1"


def test_service_log_paths_fall_back_to_bundle_runtime_logs(tmp_path: Path, monkeypatch) -> None:
    bundle_root = tmp_path / "bundle"
    monkeypatch.setattr(
        windows_pilot_services,
        "_service_runtime_option",
        lambda service_name, option_name, default=None: (
            None if option_name == "log_dir" else str(bundle_root) if option_name == "bundle_root" else default
        ),
    )

    stdout_path, stderr_path = windows_pilot_services._service_log_paths("svc-demo", "demo_shell")

    assert stdout_path == bundle_root / "runtime" / "logs" / "demo_shell.stdout.log"
    assert stderr_path == bundle_root / "runtime" / "logs" / "demo_shell.stderr.log"
    assert stdout_path.parent.exists()
