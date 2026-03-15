from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_windows_customer_pilot.py"
)
SPEC = importlib.util.spec_from_file_location("bootstrap_windows_customer_pilot_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _write_minimal_bundle_root(root: Path) -> None:
    (root / "runtime" / "config").mkdir(parents=True, exist_ok=True)
    (root / "runtime" / "requirements-pilot.txt").write_text("Django>=5.2,<5.3\n", encoding="utf-8")
    (root / "runtime" / "config" / "runtime_environments.yml").write_text(
        "default_environment: container\nenvironments: {}\n",
        encoding="utf-8",
    )
    (root / "tools").mkdir(parents=True, exist_ok=True)
    (root / "tools" / "rebuild_demo_shell.py").write_text("print('stub')\n", encoding="utf-8")
    (root / "seed_dataset").mkdir(parents=True, exist_ok=True)
    (root / "seed_dataset" / "manifest.yml").write_text("manifest_version: 1\n", encoding="utf-8")
    (root / "pilot_manifest.json").write_text(
        json.dumps(
            {
                "pilot_name": "public-safety-regressions",
                "source_run_id": "RUN-SOURCE-001",
            }
        )
        + "\n",
        encoding="utf-8",
    )


def test_bootstrap_windows_customer_pilot_prepares_runtime_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle_root = tmp_path / "pilot"
    bundle_root.mkdir(parents=True, exist_ok=True)
    _write_minimal_bundle_root(bundle_root)

    venv_python = bundle_root / ".venv" / "Scripts" / "python.exe"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("", encoding="utf-8")

    monkeypatch.setattr(
        MODULE,
        "ensure_virtualenv",
        lambda *, bundle_root, python_executable: venv_python,
    )
    install_calls: list[tuple[Path, Path]] = []
    monkeypatch.setattr(
        MODULE,
        "install_runtime_requirements",
        lambda *, venv_python, bundle_root: install_calls.append((venv_python, bundle_root)),
    )
    monkeypatch.setattr(
        MODULE,
        "ensure_postgresql_container",
        lambda **kwargs: MODULE.PostgreSQLPilotRuntime(
            container_name="etl-identity-pilot-public-safety-regressions",
            host="127.0.0.1",
            port=55432,
            database_name="identity_state",
            username="etl_identity",
            password="pilot-password",
            image="postgres:16-alpine",
            state_db="postgresql+psycopg://etl_identity:pilot-password@127.0.0.1:55432/identity_state",
        ),
    )
    upgrade_calls: list[str] = []
    monkeypatch.setattr(
        MODULE,
        "upgrade_postgresql_state_store",
        lambda **kwargs: upgrade_calls.append(kwargs["state_db"]),
    )
    seed_calls: list[str] = []
    monkeypatch.setattr(
        MODULE,
        "run_seed_pipeline_against_postgresql",
        lambda **kwargs: seed_calls.append(kwargs["state_db"]),
    )
    monkeypatch.setattr(
        MODULE,
        "latest_completed_run_id",
        lambda **kwargs: "RUN-POSTGRES-001",
    )
    prepared_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        MODULE,
        "prepare_demo_shell_from_postgresql",
        lambda **kwargs: prepared_calls.append((kwargs["state_db"], kwargs["run_id"])),
    )

    result = MODULE.bootstrap_windows_customer_pilot(
        bundle=None,
        bundle_root=str(bundle_root),
        install_root=None,
        python_executable="python",
        postgres_port=None,
        postgres_container_name=None,
        postgres_db="identity_state",
        postgres_user="etl_identity",
        postgres_password="pilot-password",
        demo_host="127.0.0.1",
        demo_port=8000,
        service_port=8010,
        prepare_only=True,
    )

    state_db = "postgresql+psycopg://etl_identity:pilot-password@127.0.0.1:55432/identity_state"
    assert install_calls == [(venv_python, bundle_root)]
    assert upgrade_calls == [state_db]
    assert seed_calls == [state_db]
    assert prepared_calls == [(state_db, "RUN-POSTGRES-001")]

    runtime_env = (bundle_root / "runtime" / "pilot_runtime.env").read_text(encoding="utf-8")
    assert f"ETL_IDENTITY_STATE_DB={state_db}" in runtime_env
    assert "ETL_IDENTITY_PILOT_SOURCE_RUN_ID=RUN-POSTGRES-001" in runtime_env

    bootstrap_config = json.loads(
        (bundle_root / "runtime" / "pilot_bootstrap.json").read_text(encoding="utf-8")
    )
    assert bootstrap_config["run_id"] == "RUN-POSTGRES-001"
    assert bootstrap_config["postgres_container_name"] == "etl-identity-pilot-public-safety-regressions"
    assert bootstrap_config["service_port"] == 8010

    assert (bundle_root / "launch" / "start_pilot_demo_shell.ps1").exists()
    assert (bundle_root / "launch" / "start_pilot_service.ps1").exists()
    assert (bundle_root / "launch" / "stop_pilot_postgres.ps1").exists()

    assert result.run_id == "RUN-POSTGRES-001"
    assert result.postgres_port == 55432
    assert result.demo_url == "http://127.0.0.1:8000/"
    assert result.service_url == "http://127.0.0.1:8010/"


def test_latest_completed_run_id_uses_runtime_pythonpath(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle_root = tmp_path / "pilot"
    bundle_root.mkdir(parents=True, exist_ok=True)
    _write_minimal_bundle_root(bundle_root)

    captured: dict[str, object] = {}

    def fake_capture_output(command, *, cwd=None, env=None):
        captured["command"] = list(command)
        captured["cwd"] = cwd
        captured["env"] = dict(env or {})
        return "RUN-POSTGRES-002"

    monkeypatch.setattr(MODULE, "_capture_output", fake_capture_output)

    run_id = MODULE.latest_completed_run_id(
        venv_python=bundle_root / ".venv" / "Scripts" / "python.exe",
        bundle_root=bundle_root,
        state_db="postgresql+psycopg://etl_identity:pilot-password@127.0.0.1:55432/identity_state",
    )

    assert run_id == "RUN-POSTGRES-002"
    assert captured["cwd"] == bundle_root
    pythonpath = captured["env"]["PYTHONPATH"]
    assert str(bundle_root / "runtime" / "src") in pythonpath
