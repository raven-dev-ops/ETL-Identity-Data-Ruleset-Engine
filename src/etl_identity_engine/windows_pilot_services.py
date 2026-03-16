"""Windows service helpers for the supported single-host customer pilot."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import subprocess
from typing import Literal


DEMO_SHELL_SERVICE_NAME = "ETLIdentityPilotDemoShell"
SERVICE_API_SERVICE_NAME = "ETLIdentityPilotServiceApi"
DEMO_SHELL_PYTHON_CLASS = "etl_identity_engine.windows_pilot_services.CustomerPilotDemoShellService"
SERVICE_API_PYTHON_CLASS = "etl_identity_engine.windows_pilot_services.CustomerPilotServiceApiService"

SERVICE_KIND_CHOICES = ("demo_shell", "service_api")
SERVICE_ACTION_CHOICES = (
    "install",
    "start",
    "stop",
    "restart",
    "remove",
    "status",
    "install-and-start",
    "stop-and-remove",
)
SERVICE_STATUS_LABELS = {
    1: "stopped",
    2: "start_pending",
    3: "stop_pending",
    4: "running",
    5: "continue_pending",
    6: "pause_pending",
    7: "paused",
}
WINDOWS_ERROR_SERVICE_DOES_NOT_EXIST = 1060


def _win32_modules():
    try:
        import servicemanager
        import win32event
        import win32service
        import win32serviceutil
    except ImportError as exc:  # pragma: no cover - exercised on Windows only.
        raise RuntimeError(
            "Windows pilot service support requires pywin32 and a Windows host."
        ) from exc
    return servicemanager, win32event, win32service, win32serviceutil


def _require_windows_host() -> None:
    if os.name != "nt":
        raise RuntimeError("Windows pilot service support is only available on Windows hosts.")


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


@dataclass(frozen=True)
class WindowsPilotServiceDefinition:
    kind: Literal["demo_shell", "service_api"]
    service_name: str
    display_name: str
    description: str
    python_class: str
    host: str
    port: int
    startup: Literal["manual", "auto"]


@dataclass(frozen=True)
class WindowsPilotServiceStatus:
    kind: Literal["demo_shell", "service_api"]
    service_name: str
    display_name: str
    installed: bool
    status_code: int | None
    status: str


def _service_log_dir(bundle_root: Path) -> Path:
    return bundle_root / "runtime" / "logs"


def _runtime_env_path(bundle_root: Path) -> Path:
    return bundle_root / "runtime" / "pilot_runtime.env"


def _bootstrap_config_path(bundle_root: Path) -> Path:
    return bundle_root / "runtime" / "pilot_bootstrap.json"


def _service_startup_to_value(startup: str) -> int:
    _, _, win32service, _ = _win32_modules()
    normalized = startup.strip().lower()
    if normalized == "manual":
        return win32service.SERVICE_DEMAND_START
    if normalized == "auto":
        return win32service.SERVICE_AUTO_START
    raise ValueError("Service startup must be 'manual' or 'auto'")


def load_windows_pilot_service_definitions(bundle_root: Path) -> dict[str, WindowsPilotServiceDefinition]:
    config = _read_json(_bootstrap_config_path(bundle_root))
    pilot_name = str(config.get("pilot_name", "customer-pilot")).strip() or "customer-pilot"
    demo_host = str(config.get("demo_host", "127.0.0.1")).strip() or "127.0.0.1"
    demo_port = int(config.get("demo_port", 8000))
    service_host = str(config.get("service_host", "127.0.0.1")).strip() or "127.0.0.1"
    service_port = int(config.get("service_port", 8010))
    startup = str(config.get("windows_service_startup", "manual")).strip().lower() or "manual"
    if startup not in {"manual", "auto"}:
        startup = "manual"
    return {
        "demo_shell": WindowsPilotServiceDefinition(
            kind="demo_shell",
            service_name=DEMO_SHELL_SERVICE_NAME,
            display_name=f"ETL Identity Pilot Demo Shell ({pilot_name})",
            description=(
                "Read-only Django demo shell for the ETL Identity Engine Windows single-host "
                "customer pilot."
            ),
            python_class=DEMO_SHELL_PYTHON_CLASS,
            host=demo_host,
            port=demo_port,
            startup=startup,
        ),
        "service_api": WindowsPilotServiceDefinition(
            kind="service_api",
            service_name=SERVICE_API_SERVICE_NAME,
            display_name=f"ETL Identity Pilot Service API ({pilot_name})",
            description=(
                "Authenticated operator API for the ETL Identity Engine Windows single-host "
                "customer pilot."
            ),
            python_class=SERVICE_API_PYTHON_CLASS,
            host=service_host,
            port=service_port,
            startup=startup,
        ),
    }


def _service_custom_options(
    *,
    bundle_root: Path,
    definition: WindowsPilotServiceDefinition,
) -> dict[str, str]:
    return {
        "bundle_root": str(bundle_root),
        "bootstrap_config": str(_bootstrap_config_path(bundle_root)),
        "runtime_env_path": str(_runtime_env_path(bundle_root)),
        "log_dir": str(_service_log_dir(bundle_root)),
        "host": definition.host,
        "port": str(definition.port),
        "service_kind": definition.kind,
        "display_name": definition.display_name,
    }


def _service_exists(service_name: str) -> bool:
    _, _, _, win32serviceutil = _win32_modules()
    try:
        win32serviceutil.QueryServiceStatus(service_name)
    except Exception as exc:
        error_code = getattr(exc, "winerror", None)
        if not isinstance(error_code, int):
            first_arg = exc.args[0] if exc.args else None
            if isinstance(first_arg, int):
                error_code = first_arg
        if error_code == WINDOWS_ERROR_SERVICE_DOES_NOT_EXIST:
            return False
        raise
    return True


def _service_state_code(service_name: str) -> int | None:
    _, _, _, win32serviceutil = _win32_modules()
    if not _service_exists(service_name):
        return None
    return int(win32serviceutil.QueryServiceStatus(service_name)[1])


def install_windows_pilot_service(
    *,
    bundle_root: Path,
    service_kind: Literal["demo_shell", "service_api"],
    startup: str | None = None,
) -> WindowsPilotServiceDefinition:
    _require_windows_host()
    _, _, _, win32serviceutil = _win32_modules()
    definition = load_windows_pilot_service_definitions(bundle_root)[service_kind]
    resolved_startup = startup or definition.startup
    start_type = _service_startup_to_value(resolved_startup)
    custom_options = _service_custom_options(bundle_root=bundle_root, definition=definition)
    if _service_exists(definition.service_name):
        win32serviceutil.ChangeServiceConfig(
            definition.python_class,
            definition.service_name,
            startType=start_type,
            displayName=definition.display_name,
            description=definition.description,
        )
    else:
        win32serviceutil.InstallService(
            definition.python_class,
            definition.service_name,
            definition.display_name,
            startType=start_type,
            description=definition.description,
        )
    for option_name, option_value in custom_options.items():
        win32serviceutil.SetServiceCustomOption(definition.service_name, option_name, option_value)
    return WindowsPilotServiceDefinition(
        **{
            **asdict(definition),
            "startup": resolved_startup,
        }
    )


def start_windows_pilot_service(service_kind: Literal["demo_shell", "service_api"], *, bundle_root: Path) -> None:
    _require_windows_host()
    _, _, _, win32serviceutil = _win32_modules()
    definition = load_windows_pilot_service_definitions(bundle_root)[service_kind]
    if _service_state_code(definition.service_name) == 4:
        return
    win32serviceutil.StartService(definition.service_name)


def stop_windows_pilot_service(service_kind: Literal["demo_shell", "service_api"], *, bundle_root: Path) -> None:
    _require_windows_host()
    _, _, _, win32serviceutil = _win32_modules()
    definition = load_windows_pilot_service_definitions(bundle_root)[service_kind]
    current_state = _service_state_code(definition.service_name)
    if current_state is None or current_state == 1:
        return
    win32serviceutil.StopService(definition.service_name)


def remove_windows_pilot_service(service_kind: Literal["demo_shell", "service_api"], *, bundle_root: Path) -> None:
    _require_windows_host()
    _, _, _, win32serviceutil = _win32_modules()
    definition = load_windows_pilot_service_definitions(bundle_root)[service_kind]
    if not _service_exists(definition.service_name):
        return
    win32serviceutil.RemoveService(definition.service_name)


def query_windows_pilot_service_status(
    service_kind: Literal["demo_shell", "service_api"],
    *,
    bundle_root: Path,
) -> WindowsPilotServiceStatus:
    _require_windows_host()
    definition = load_windows_pilot_service_definitions(bundle_root)[service_kind]
    if not _service_exists(definition.service_name):
        return WindowsPilotServiceStatus(
            kind=definition.kind,
            service_name=definition.service_name,
            display_name=definition.display_name,
            installed=False,
            status_code=None,
            status="not_installed",
        )
    status = _service_state_code(definition.service_name)
    assert status is not None
    return WindowsPilotServiceStatus(
        kind=definition.kind,
        service_name=definition.service_name,
        display_name=definition.display_name,
        installed=True,
        status_code=int(status),
        status=SERVICE_STATUS_LABELS.get(int(status), f"status_{int(status)}"),
    )


def manage_windows_pilot_services(
    *,
    bundle_root: Path,
    action: Literal[
        "install",
        "start",
        "stop",
        "restart",
        "remove",
        "status",
        "install-and-start",
        "stop-and-remove",
    ],
    service_kind: Literal["demo_shell", "service_api", "all"] = "all",
    startup: str | None = None,
) -> dict[str, object]:
    target_kinds = SERVICE_KIND_CHOICES if service_kind == "all" else (service_kind,)
    results: list[dict[str, object]] = []
    for kind in target_kinds:
        if action == "install":
            definition = install_windows_pilot_service(
                bundle_root=bundle_root,
                service_kind=kind,
                startup=startup,
            )
            results.append({"kind": kind, "action": "install", "definition": asdict(definition)})
        elif action == "start":
            start_windows_pilot_service(kind, bundle_root=bundle_root)
            results.append({"kind": kind, "action": "start"})
        elif action == "stop":
            stop_windows_pilot_service(kind, bundle_root=bundle_root)
            results.append({"kind": kind, "action": "stop"})
        elif action == "restart":
            stop_windows_pilot_service(kind, bundle_root=bundle_root)
            start_windows_pilot_service(kind, bundle_root=bundle_root)
            results.append({"kind": kind, "action": "restart"})
        elif action == "remove":
            remove_windows_pilot_service(kind, bundle_root=bundle_root)
            results.append({"kind": kind, "action": "remove"})
        elif action == "install-and-start":
            definition = install_windows_pilot_service(
                bundle_root=bundle_root,
                service_kind=kind,
                startup=startup,
            )
            start_windows_pilot_service(kind, bundle_root=bundle_root)
            results.append(
                {
                    "kind": kind,
                    "action": "install-and-start",
                    "definition": asdict(definition),
                }
            )
        elif action == "stop-and-remove":
            stop_windows_pilot_service(kind, bundle_root=bundle_root)
            remove_windows_pilot_service(kind, bundle_root=bundle_root)
            results.append({"kind": kind, "action": "stop-and-remove"})
        elif action == "status":
            results.append(
                {
                    "kind": kind,
                    "action": "status",
                    "status": asdict(query_windows_pilot_service_status(kind, bundle_root=bundle_root)),
                }
            )
        else:  # pragma: no cover - guarded by argparse.
            raise ValueError(f"Unsupported Windows pilot service action: {action}")

    if action != "status":
        for kind in target_kinds:
            results.append(
                {
                    "kind": kind,
                    "action": "status",
                    "status": asdict(query_windows_pilot_service_status(kind, bundle_root=bundle_root)),
                }
            )
    return {
        "bundle_root": str(bundle_root),
        "action": action,
        "service_kind": service_kind,
        "results": results,
    }


def _service_runtime_option(service_name: str, option_name: str, *, default: str | None = None) -> str | None:
    _, _, _, win32serviceutil = _win32_modules()
    return win32serviceutil.GetServiceCustomOption(service_name, option_name, default)


def _service_bundle_root(service_name: str) -> Path:
    value = _service_runtime_option(service_name, "bundle_root")
    if not value:
        raise RuntimeError(f"Windows pilot service {service_name} is missing bundle_root metadata")
    return Path(str(value)).resolve()


def _service_bootstrap_config(service_name: str) -> dict[str, object]:
    bootstrap_config = _service_runtime_option(service_name, "bootstrap_config")
    if not bootstrap_config:
        raise RuntimeError(
            f"Windows pilot service {service_name} is missing bootstrap_config metadata"
        )
    return _read_json(Path(str(bootstrap_config)))


def _service_runtime_env(service_name: str) -> dict[str, str]:
    runtime_env_path = _service_runtime_option(service_name, "runtime_env_path")
    if not runtime_env_path:
        return {}
    return _read_env_file(Path(str(runtime_env_path)))


def _service_log_paths(service_name: str, service_kind: str) -> tuple[Path, Path]:
    log_dir = _service_runtime_option(service_name, "log_dir")
    if not log_dir:
        bundle_root = _service_bundle_root(service_name)
        resolved_log_dir = _service_log_dir(bundle_root)
    else:
        resolved_log_dir = Path(str(log_dir)).resolve()
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    return (
        resolved_log_dir / f"{service_kind}.stdout.log",
        resolved_log_dir / f"{service_kind}.stderr.log",
    )


def _service_subprocess_command(service_name: str, service_kind: str) -> tuple[list[str], Path, dict[str, str]]:
    bundle_root = _service_bundle_root(service_name)
    bootstrap_config = _service_bootstrap_config(service_name)
    env = os.environ.copy()
    env.update(_service_runtime_env(service_name))
    env.setdefault("PYTHONUNBUFFERED", "1")

    python_executable = bundle_root / ".venv" / "Scripts" / "python.exe"
    if not python_executable.exists():
        raise FileNotFoundError(
            f"Windows pilot virtualenv was not found for service {service_name}: {python_executable}"
        )

    if service_kind == "demo_shell":
        command = [
            str(python_executable),
            str(bundle_root / "tools" / "rebuild_demo_shell.py"),
            "--state-db",
            str(bootstrap_config["state_db"]),
            "--run-id",
            str(bootstrap_config["run_id"]),
            "--host",
            str(_service_runtime_option(service_name, "host", str(bootstrap_config.get("demo_host", "127.0.0.1")))),
            "--port",
            str(_service_runtime_option(service_name, "port", str(bootstrap_config.get("demo_port", 8000)))),
        ]
    elif service_kind == "service_api":
        command = [
            str(python_executable),
            "-m",
            "etl_identity_engine.cli",
            "serve-api",
            "--environment",
            "container",
            "--runtime-config",
            str(bundle_root / "runtime" / "config" / "runtime_environments.yml"),
            "--state-db",
            str(bootstrap_config["state_db"]),
            "--host",
            str(_service_runtime_option(service_name, "host", str(bootstrap_config.get("service_host", "127.0.0.1")))),
            "--port",
            str(_service_runtime_option(service_name, "port", str(bootstrap_config.get("service_port", 8010)))),
        ]
    else:  # pragma: no cover - service kinds are fixed.
        raise ValueError(f"Unsupported service kind: {service_kind}")
    return command, bundle_root, env


def _run_subprocess_service(service_name: str, service_kind: str, stop_event) -> None:
    servicemanager, win32event, _, _ = _win32_modules()
    command, cwd, env = _service_subprocess_command(service_name, service_kind)
    stdout_path, stderr_path = _service_log_paths(service_name, service_kind)
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    with stdout_path.open("a", encoding="utf-8") as stdout_handle, stderr_path.open(
        "a",
        encoding="utf-8",
    ) as stderr_handle:
        stdout_handle.write(f"[service-start] {' '.join(command)}\n")
        stdout_handle.flush()
        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=env,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
            creationflags=creationflags,
        )
        servicemanager.LogInfoMsg(
            f"Started ETL Identity pilot subprocess for {service_name}: {' '.join(command)}"
        )
        try:
            while True:
                if process.poll() is not None:
                    exit_code = int(process.returncode or 0)
                    if exit_code != 0:
                        servicemanager.LogErrorMsg(
                            f"ETL Identity pilot subprocess {service_name} exited with code {exit_code}"
                        )
                    break
                if win32event.WaitForSingleObject(stop_event, 1000) == win32event.WAIT_OBJECT_0:
                    break
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=20)


if os.name == "nt":  # pragma: no branch - Windows-only service host.
    try:
        servicemanager, win32event, win32service, win32serviceutil = _win32_modules()
    except RuntimeError:  # pragma: no cover - import guard.
        servicemanager = win32event = win32service = win32serviceutil = None
    else:

        class _CustomerPilotProcessService(win32serviceutil.ServiceFramework):
            SERVICE_KIND = ""
            _svc_name_ = "ETLIdentityPilotBase"
            _svc_display_name_ = "ETL Identity Pilot Base"
            _svc_description_ = "Base service class for ETL Identity pilot services."

            def __init__(self, args):
                super().__init__(args)
                self.stop_event = win32event.CreateEvent(None, 0, 0, None)

            def SvcStop(self):  # pragma: no cover - Windows service path.
                self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
                win32event.SetEvent(self.stop_event)

            def SvcDoRun(self):  # pragma: no cover - Windows service path.
                _run_subprocess_service(self._svc_name_, self.SERVICE_KIND, self.stop_event)


        class CustomerPilotDemoShellService(_CustomerPilotProcessService):
            SERVICE_KIND = "demo_shell"
            _svc_name_ = DEMO_SHELL_SERVICE_NAME
            _svc_display_name_ = "ETL Identity Pilot Demo Shell"
            _svc_description_ = (
                "Read-only Django demo shell for the ETL Identity Engine Windows single-host customer pilot."
            )


        class CustomerPilotServiceApiService(_CustomerPilotProcessService):
            SERVICE_KIND = "service_api"
            _svc_name_ = SERVICE_API_SERVICE_NAME
            _svc_display_name_ = "ETL Identity Pilot Service API"
            _svc_description_ = (
                "Authenticated operator API for the ETL Identity Engine Windows single-host customer pilot."
            )
