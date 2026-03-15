from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from typing import Sequence
import zipfile


DEFAULT_POSTGRES_DB = "identity_state"
DEFAULT_POSTGRES_USER = "etl_identity"
DEFAULT_POSTGRES_PASSWORD = "pilot-password"
DEFAULT_POSTGRES_IMAGE = "postgres:16-alpine"
DEFAULT_POSTGRES_HOST = "127.0.0.1"
DEFAULT_DEMO_HOST = "127.0.0.1"
DEFAULT_SERVICE_HOST = "127.0.0.1"
DEFAULT_DEMO_PORT = 8000
DEFAULT_SERVICE_PORT = 8010
DEFAULT_SERVICE_READER_API_KEY = "pilot-reader-key"
DEFAULT_SERVICE_OPERATOR_API_KEY = "pilot-operator-key"
BOOTSTRAP_CONFIG_RELATIVE_PATH = Path("runtime") / "pilot_bootstrap.json"
BOOTSTRAP_ENV_RELATIVE_PATH = Path("runtime") / "pilot_runtime.env"


@dataclass(frozen=True)
class PilotBundleContext:
    bundle_root: Path
    pilot_name: str
    source_run_id: str


@dataclass(frozen=True)
class PostgreSQLPilotRuntime:
    container_name: str
    host: str
    port: int
    database_name: str
    username: str
    password: str
    image: str
    state_db: str


@dataclass(frozen=True)
class WindowsPilotBootstrapResult:
    bundle_root: Path
    venv_python: Path
    runtime_env_path: Path
    bootstrap_config_path: Path
    state_db: str
    postgres_container_name: str
    postgres_port: int
    run_id: str
    demo_url: str
    service_url: str


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bootstrap the Windows-first single-host customer pilot baseline "
            "for the standalone public-safety demo bundle."
        )
    )
    parser.add_argument("--bundle", default=None)
    parser.add_argument("--bundle-root", default=None)
    parser.add_argument("--install-root", default=None)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--postgres-port", default=None, type=int)
    parser.add_argument("--postgres-container-name", default=None)
    parser.add_argument("--postgres-db", default=DEFAULT_POSTGRES_DB)
    parser.add_argument("--postgres-user", default=DEFAULT_POSTGRES_USER)
    parser.add_argument("--postgres-password", default=DEFAULT_POSTGRES_PASSWORD)
    parser.add_argument("--demo-host", default=DEFAULT_DEMO_HOST)
    parser.add_argument("--demo-port", default=DEFAULT_DEMO_PORT, type=int)
    parser.add_argument("--service-port", default=DEFAULT_SERVICE_PORT, type=int)
    parser.add_argument("--prepare-only", action="store_true")
    return parser.parse_args(argv)


def _run(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(command),
            cwd=None if cwd is None else str(cwd),
            env=env,
            check=True,
            capture_output=capture_output,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Required executable {command[0]!r} was not found") from exc
    except subprocess.CalledProcessError as exc:
        detail = "\n".join(
            part for part in (exc.stdout.strip(), exc.stderr.strip()) if part
        ).strip()
        if detail:
            raise RuntimeError(f"Command failed: {' '.join(command)}\n{detail}") from exc
        raise RuntimeError(f"Command failed: {' '.join(command)}") from exc


def _capture_output(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> str:
    completed = _run(command, cwd=cwd, env=env, capture_output=True)
    return completed.stdout.strip()


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((DEFAULT_POSTGRES_HOST, 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_bundle(bundle_path: Path, install_root: Path) -> Path:
    if install_root.exists():
        existing = sorted(install_root.iterdir())
        if existing:
            raise ValueError(
                f"Install root already exists and is not empty: {install_root}"
            )
    else:
        install_root.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle_path) as archive:
        archive.extractall(install_root)
    return install_root


def _resolve_bundle_root(
    *,
    bundle: str | None,
    bundle_root: str | None,
    install_root: str | None,
) -> Path:
    if bundle and bundle_root:
        raise ValueError("--bundle and --bundle-root are mutually exclusive")

    if bundle is not None:
        bundle_path = Path(bundle).resolve()
        if not bundle_path.exists():
            raise FileNotFoundError(f"Customer pilot bundle not found: {bundle_path}")
        resolved_install_root = (
            Path(install_root).resolve()
            if install_root is not None
            else bundle_path.with_suffix("")
        )
        return _extract_bundle(bundle_path, resolved_install_root)

    if bundle_root is not None:
        resolved = Path(bundle_root).resolve()
    else:
        resolved = Path(__file__).resolve().parents[1]

    if not (resolved / "pilot_manifest.json").exists():
        raise FileNotFoundError(
            "Unable to locate an extracted customer pilot bundle root. "
            "Provide --bundle or --bundle-root."
        )
    return resolved


def resolve_pilot_bundle_context(
    *,
    bundle: str | None,
    bundle_root: str | None,
    install_root: str | None,
) -> PilotBundleContext:
    resolved_bundle_root = _resolve_bundle_root(
        bundle=bundle,
        bundle_root=bundle_root,
        install_root=install_root,
    )
    pilot_manifest = _load_json(resolved_bundle_root / "pilot_manifest.json")
    pilot_name = str(pilot_manifest.get("pilot_name", "") or "").strip()
    if not pilot_name:
        raise ValueError("pilot_manifest.json is missing pilot_name")
    source_run_id = str(pilot_manifest.get("source_run_id", "") or "").strip()
    if not source_run_id:
        raise ValueError("pilot_manifest.json is missing source_run_id")
    return PilotBundleContext(
        bundle_root=resolved_bundle_root,
        pilot_name=pilot_name,
        source_run_id=source_run_id,
    )


def _venv_python_path(bundle_root: Path) -> Path:
    return bundle_root / ".venv" / "Scripts" / "python.exe"


def ensure_virtualenv(*, bundle_root: Path, python_executable: str) -> Path:
    venv_python = _venv_python_path(bundle_root)
    if not venv_python.exists():
        _run([python_executable, "-m", "venv", str(bundle_root / ".venv")])
    if not venv_python.exists():
        raise FileNotFoundError(f"Expected venv python was not created: {venv_python}")
    return venv_python


def install_runtime_requirements(*, venv_python: Path, bundle_root: Path) -> None:
    requirements_path = bundle_root / "runtime" / "requirements-pilot.txt"
    if not requirements_path.exists():
        raise FileNotFoundError(f"Pilot requirements file not found: {requirements_path}")
    _run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
    _run([str(venv_python), "-m", "pip", "install", "-r", str(requirements_path)])
    _run([str(venv_python), "-m", "pip", "install", "--no-deps", str(bundle_root / "runtime")])


def _docker_inspect_state(container_name: str) -> str | None:
    try:
        return _capture_output(
            ["docker", "inspect", "-f", "{{.State.Status}}", container_name]
        ).strip()
    except RuntimeError:
        return None


def wait_for_postgresql_container(
    *,
    container_name: str,
    username: str,
    database_name: str,
    timeout_seconds: int = 60,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        completed = subprocess.run(
            ["docker", "exec", container_name, "pg_isready", "-U", username, "-d", database_name],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if completed.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("Timed out waiting for PostgreSQL pilot container readiness")


def ensure_postgresql_container(
    *,
    pilot_name: str,
    port: int | None,
    container_name: str | None,
    database_name: str,
    username: str,
    password: str,
    image: str = DEFAULT_POSTGRES_IMAGE,
) -> PostgreSQLPilotRuntime:
    resolved_port = port if port is not None else _find_free_tcp_port()
    resolved_container_name = container_name or f"etl-identity-pilot-{pilot_name}"
    state = _docker_inspect_state(resolved_container_name)
    if state == "running":
        wait_for_postgresql_container(
            container_name=resolved_container_name,
            username=username,
            database_name=database_name,
        )
    elif state is not None:
        _run(["docker", "start", resolved_container_name])
        wait_for_postgresql_container(
            container_name=resolved_container_name,
            username=username,
            database_name=database_name,
        )
    else:
        _run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                resolved_container_name,
                "-p",
                f"{resolved_port}:5432",
                "-e",
                f"POSTGRES_DB={database_name}",
                "-e",
                f"POSTGRES_USER={username}",
                "-e",
                f"POSTGRES_PASSWORD={password}",
                image,
            ]
        )
        wait_for_postgresql_container(
            container_name=resolved_container_name,
            username=username,
            database_name=database_name,
        )

    state_db = (
        f"postgresql+psycopg://{username}:{password}@{DEFAULT_POSTGRES_HOST}:{resolved_port}/{database_name}"
    )
    return PostgreSQLPilotRuntime(
        container_name=resolved_container_name,
        host=DEFAULT_POSTGRES_HOST,
        port=resolved_port,
        database_name=database_name,
        username=username,
        password=password,
        image=image,
        state_db=state_db,
    )


def _runtime_env(bundle_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    src_path = str(bundle_root / "runtime" / "src")
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        src_path if not existing_pythonpath else os.pathsep.join((src_path, existing_pythonpath))
    )
    return env


def _run_runtime_cli(
    *,
    venv_python: Path,
    bundle_root: Path,
    args: Sequence[str],
) -> None:
    _run(
        [str(venv_python), "-m", "etl_identity_engine.cli", *args],
        cwd=bundle_root,
        env=_runtime_env(bundle_root),
    )


def upgrade_postgresql_state_store(
    *,
    venv_python: Path,
    bundle_root: Path,
    state_db: str,
) -> None:
    _run_runtime_cli(
        venv_python=venv_python,
        bundle_root=bundle_root,
        args=("state-db-upgrade", "--state-db", state_db),
    )


def run_seed_pipeline_against_postgresql(
    *,
    venv_python: Path,
    bundle_root: Path,
    state_db: str,
) -> None:
    _run_runtime_cli(
        venv_python=venv_python,
        bundle_root=bundle_root,
        args=(
            "run-all",
            "--base-dir",
            str(bundle_root / "seed_run_postgresql"),
            "--manifest",
            str(bundle_root / "seed_dataset" / "manifest.yml"),
            "--state-db",
            state_db,
            "--config-dir",
            str(bundle_root / "runtime" / "config"),
        ),
    )


def latest_completed_run_id(
    *,
    venv_python: Path,
    bundle_root: Path,
    state_db: str,
) -> str:
    code = (
        "from etl_identity_engine.storage.sqlite_store import PipelineStateStore; "
        f"store = PipelineStateStore(r'''{state_db}'''); "
        "run_id = store.latest_completed_run_id() or ''; "
        "print(run_id); "
        "store.engine.dispose()"
    )
    run_id = _capture_output(
        [str(venv_python), "-c", code],
        cwd=bundle_root,
        env=_runtime_env(bundle_root),
    )
    if not run_id:
        raise RuntimeError("Unable to resolve a completed run ID from the PostgreSQL pilot state store")
    return run_id


def prepare_demo_shell_from_postgresql(
    *,
    venv_python: Path,
    bundle_root: Path,
    state_db: str,
    run_id: str,
) -> None:
    _run(
        [
            str(venv_python),
            str(bundle_root / "tools" / "rebuild_demo_shell.py"),
            "--state-db",
            state_db,
            "--run-id",
            run_id,
            "--output-dir",
            str(bundle_root / "demo_shell"),
            "--prepare-only",
        ],
        cwd=bundle_root,
        env=_runtime_env(bundle_root),
    )


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def write_pilot_runtime_env(
    *,
    bundle_root: Path,
    pilot_name: str,
    state_db: str,
    run_id: str,
    runtime: PostgreSQLPilotRuntime,
    demo_host: str,
    demo_port: int,
    service_host: str,
    service_port: int,
) -> Path:
    env_path = bundle_root / BOOTSTRAP_ENV_RELATIVE_PATH
    _write_text(
        env_path,
        "\n".join(
            [
                f"ETL_IDENTITY_PILOT_NAME={pilot_name}",
                f"ETL_IDENTITY_STATE_DB={state_db}",
                f"ETL_IDENTITY_PILOT_SOURCE_RUN_ID={run_id}",
                f"ETL_IDENTITY_PILOT_POSTGRES_CONTAINER={runtime.container_name}",
                f"ETL_IDENTITY_PILOT_POSTGRES_PORT={runtime.port}",
                f"ETL_IDENTITY_SERVICE_READER_API_KEY={DEFAULT_SERVICE_READER_API_KEY}",
                f"ETL_IDENTITY_SERVICE_OPERATOR_API_KEY={DEFAULT_SERVICE_OPERATOR_API_KEY}",
                f"ETL_IDENTITY_PILOT_DEMO_HOST={demo_host}",
                f"ETL_IDENTITY_PILOT_DEMO_PORT={demo_port}",
                f"ETL_IDENTITY_PILOT_SERVICE_HOST={service_host}",
                f"ETL_IDENTITY_PILOT_SERVICE_PORT={service_port}",
            ]
        )
        + "\n",
    )
    return env_path


def write_pilot_bootstrap_config(
    *,
    bundle_root: Path,
    pilot_name: str,
    runtime: PostgreSQLPilotRuntime,
    run_id: str,
    demo_host: str,
    demo_port: int,
    service_host: str,
    service_port: int,
) -> Path:
    config_path = bundle_root / BOOTSTRAP_CONFIG_RELATIVE_PATH
    log_dir = bundle_root / "runtime" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "pilot_name": pilot_name,
        "bundle_root": str(bundle_root),
        "state_db": runtime.state_db,
        "run_id": run_id,
        "postgres_container_name": runtime.container_name,
        "postgres_host": runtime.host,
        "postgres_port": runtime.port,
        "postgres_db": runtime.database_name,
        "postgres_user": runtime.username,
        "postgres_password": runtime.password,
        "postgres_image": runtime.image,
        "demo_host": demo_host,
        "demo_port": demo_port,
        "service_host": service_host,
        "service_port": service_port,
        "runtime_config": str(bundle_root / "runtime" / "config" / "runtime_environments.yml"),
        "runtime_env": str(bundle_root / BOOTSTRAP_ENV_RELATIVE_PATH),
        "log_dir": str(log_dir),
        "reader_api_key": DEFAULT_SERVICE_READER_API_KEY,
        "operator_api_key": DEFAULT_SERVICE_OPERATOR_API_KEY,
        "windows_service_startup": "manual",
        "windows_services": {
            "demo_shell": {
                "service_name": "ETLIdentityPilotDemoShell",
                "display_name": f"ETL Identity Pilot Demo Shell ({pilot_name})",
                "host": demo_host,
                "port": demo_port,
            },
            "service_api": {
                "service_name": "ETLIdentityPilotServiceApi",
                "display_name": f"ETL Identity Pilot Service API ({pilot_name})",
                "host": service_host,
                "port": service_port,
            },
        },
    }
    _write_text(config_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return config_path


def write_windows_launchers(*, bundle_root: Path) -> tuple[Path, Path, Path, Path]:
    launch_root = bundle_root / "launch"
    launch_root.mkdir(parents=True, exist_ok=True)

    demo_shell_path = launch_root / "start_pilot_demo_shell.ps1"
    _write_text(
        demo_shell_path,
        """param(
    [string]$Host = "",
    [int]$Port = 0
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$config = Get-Content (Join-Path $root "runtime\\pilot_bootstrap.json") | ConvertFrom-Json
$python = Join-Path $root ".venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    throw "Pilot venv python was not found. Run .\\launch\\bootstrap_windows_pilot.ps1 first."
}
if ([string]::IsNullOrWhiteSpace($Host)) { $Host = [string]$config.demo_host }
if ($Port -le 0) { $Port = [int]$config.demo_port }
& $python (Join-Path $root "tools\\rebuild_demo_shell.py") --state-db ([string]$config.state_db) --run-id ([string]$config.run_id) --host $Host --port $Port
""",
    )

    service_path = launch_root / "start_pilot_service.ps1"
    _write_text(
        service_path,
        """param(
    [string]$Host = "127.0.0.1",
    [int]$Port = 0
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$config = Get-Content (Join-Path $root "runtime\\pilot_bootstrap.json") | ConvertFrom-Json
$python = Join-Path $root ".venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    throw "Pilot venv python was not found. Run .\\launch\\bootstrap_windows_pilot.ps1 first."
}
if ($Port -le 0) { $Port = [int]$config.service_port }
$env:ETL_IDENTITY_STATE_DB = [string]$config.state_db
$env:ETL_IDENTITY_SERVICE_READER_API_KEY = [string]$config.reader_api_key
$env:ETL_IDENTITY_SERVICE_OPERATOR_API_KEY = [string]$config.operator_api_key
& $python -m etl_identity_engine.cli serve-api --environment container --runtime-config (Join-Path $root "runtime\\config\\runtime_environments.yml") --state-db ([string]$config.state_db) --host $Host --port $Port
""",
    )

    manage_services_path = launch_root / "manage_pilot_services.ps1"
    _write_text(
        manage_services_path,
        """param(
    [ValidateSet("install", "start", "stop", "restart", "remove", "status", "install-and-start", "stop-and-remove")]
    [string]$Action = "status",
    [ValidateSet("demo_shell", "service_api", "all")]
    [string]$ServiceKind = "all",
    [ValidateSet("manual", "auto")]
    [string]$Startup = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    throw "Pilot venv python was not found. Run .\\launch\\bootstrap_windows_pilot.ps1 first."
}
$arguments = @((Join-Path $root "tools\\manage_windows_pilot_services.py"), "--bundle-root", $root, "--service-kind", $ServiceKind)
if (-not [string]::IsNullOrWhiteSpace($Startup)) { $arguments += @("--startup", $Startup) }
$arguments += $Action
& $python @arguments
""",
    )

    stop_postgres_path = launch_root / "stop_pilot_postgres.ps1"
    _write_text(
        stop_postgres_path,
        """$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$config = Get-Content (Join-Path $root "runtime\\pilot_bootstrap.json") | ConvertFrom-Json
docker rm -f ([string]$config.postgres_container_name)
""",
    )
    return demo_shell_path, service_path, manage_services_path, stop_postgres_path


def bootstrap_windows_customer_pilot(
    *,
    bundle: str | None,
    bundle_root: str | None,
    install_root: str | None,
    python_executable: str,
    postgres_port: int | None,
    postgres_container_name: str | None,
    postgres_db: str,
    postgres_user: str,
    postgres_password: str,
    demo_host: str,
    demo_port: int,
    service_port: int,
    prepare_only: bool,
) -> WindowsPilotBootstrapResult:
    context = resolve_pilot_bundle_context(
        bundle=bundle,
        bundle_root=bundle_root,
        install_root=install_root,
    )
    resolved_bundle_root = context.bundle_root
    venv_python = ensure_virtualenv(
        bundle_root=resolved_bundle_root,
        python_executable=python_executable,
    )
    install_runtime_requirements(venv_python=venv_python, bundle_root=resolved_bundle_root)
    runtime = ensure_postgresql_container(
        pilot_name=context.pilot_name,
        port=postgres_port,
        container_name=postgres_container_name,
        database_name=postgres_db,
        username=postgres_user,
        password=postgres_password,
    )
    upgrade_postgresql_state_store(
        venv_python=venv_python,
        bundle_root=resolved_bundle_root,
        state_db=runtime.state_db,
    )
    run_seed_pipeline_against_postgresql(
        venv_python=venv_python,
        bundle_root=resolved_bundle_root,
        state_db=runtime.state_db,
    )
    run_id = latest_completed_run_id(
        venv_python=venv_python,
        bundle_root=resolved_bundle_root,
        state_db=runtime.state_db,
    )
    prepare_demo_shell_from_postgresql(
        venv_python=venv_python,
        bundle_root=resolved_bundle_root,
        state_db=runtime.state_db,
        run_id=run_id,
    )
    runtime_env_path = write_pilot_runtime_env(
        bundle_root=resolved_bundle_root,
        pilot_name=context.pilot_name,
        state_db=runtime.state_db,
        run_id=run_id,
        runtime=runtime,
        demo_host=demo_host,
        demo_port=demo_port,
        service_host=DEFAULT_SERVICE_HOST,
        service_port=service_port,
    )
    bootstrap_config_path = write_pilot_bootstrap_config(
        bundle_root=resolved_bundle_root,
        pilot_name=context.pilot_name,
        runtime=runtime,
        run_id=run_id,
        demo_host=demo_host,
        demo_port=demo_port,
        service_host=DEFAULT_SERVICE_HOST,
        service_port=service_port,
    )
    write_windows_launchers(bundle_root=resolved_bundle_root)

    result = WindowsPilotBootstrapResult(
        bundle_root=resolved_bundle_root,
        venv_python=venv_python,
        runtime_env_path=runtime_env_path,
        bootstrap_config_path=bootstrap_config_path,
        state_db=runtime.state_db,
        postgres_container_name=runtime.container_name,
        postgres_port=runtime.port,
        run_id=run_id,
        demo_url=f"http://{demo_host}:{demo_port}/",
        service_url=f"http://127.0.0.1:{service_port}/",
    )

    if not prepare_only:
        _run(
            [
                str(venv_python),
                str(resolved_bundle_root / "tools" / "rebuild_demo_shell.py"),
                "--state-db",
                runtime.state_db,
                "--run-id",
                run_id,
                "--host",
                demo_host,
                "--port",
                str(demo_port),
            ],
            cwd=resolved_bundle_root,
            env=_runtime_env(resolved_bundle_root),
        )

    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = bootstrap_windows_customer_pilot(
        bundle=args.bundle,
        bundle_root=args.bundle_root,
        install_root=args.install_root,
        python_executable=args.python,
        postgres_port=args.postgres_port,
        postgres_container_name=args.postgres_container_name,
        postgres_db=args.postgres_db,
        postgres_user=args.postgres_user,
        postgres_password=args.postgres_password,
        demo_host=args.demo_host,
        demo_port=args.demo_port,
        service_port=args.service_port,
        prepare_only=args.prepare_only,
    )
    print(json.dumps(asdict(result), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
