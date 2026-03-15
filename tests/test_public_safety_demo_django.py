from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3
import shutil
import sys


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

RUN_SCRIPT_PATH = SCRIPTS_DIR / "run_public_safety_demo_shell.py"
RUN_SPEC = importlib.util.spec_from_file_location("run_public_safety_demo_shell_script", RUN_SCRIPT_PATH)
assert RUN_SPEC and RUN_SPEC.loader
RUN_MODULE = importlib.util.module_from_spec(RUN_SPEC)
sys.modules[RUN_SPEC.name] = RUN_MODULE
RUN_SPEC.loader.exec_module(RUN_MODULE)

_PREPARED_SHELL = None


def _prepared_shell(tmp_path_factory):
    global _PREPARED_SHELL
    if _PREPARED_SHELL is None:
        workspace = tmp_path_factory.mktemp("public-safety-demo-django")
        _PREPARED_SHELL = RUN_MODULE.prepare_public_safety_demo_shell_workspace(
            output_dir=workspace / "shell",
            bundle=None,
            state_db=None,
            run_id=None,
            profile="small",
            seed=42,
            formats=("csv", "parquet"),
            version="0.6.0",
            host="127.0.0.1",
            port=8042,
        )
    return _PREPARED_SHELL


def test_prepare_public_safety_demo_shell_loads_bundle_into_sqlite(tmp_path_factory) -> None:
    prepared = _prepared_shell(tmp_path_factory)

    assert prepared.db_path.exists()
    assert (prepared.bundle_root / "data" / "public_safety_demo" / "public_safety_demo_summary.json").exists()
    assert (prepared.bundle_root / prepared.bundle_path.name).exists()

    with sqlite3.connect(prepared.db_path) as connection:
        table_names = {
            row[0]
            for row in connection.execute("select name from sqlite_master where type = 'table'")
        }
        assert {
            "demo_shell_demorun",
            "demo_shell_demoscenario",
            "demo_shell_goldenpersonactivity",
            "demo_shell_incidentidentity",
        } <= table_names

        demo_run_row = connection.execute(
            "select incident_count, cross_system_golden_person_count from demo_shell_demorun"
        ).fetchone()
        assert demo_run_row == (12, 4)
        assert connection.execute("select count(*) from demo_shell_demoscenario").fetchone() == (3,)


def test_public_safety_demo_shell_views_render_loaded_run(tmp_path_factory) -> None:
    _prepared_shell(tmp_path_factory)

    from django.test import Client

    client = Client()

    overview_response = client.get("/")
    assert overview_response.status_code == 200
    overview_text = overview_response.content.decode("utf-8")
    assert "ID Network Buyer Demo" in overview_text
    assert "CAD And RMS On One Identity" in overview_text
    assert "See CAD calls and RMS reports collapse into one master person." in overview_text
    assert "Regional Dispatch" in overview_text
    assert "Police Records" in overview_text

    scenario_response = client.get("/scenarios/cross_system_identity/")
    assert scenario_response.status_code == 200
    scenario_text = scenario_response.content.decode("utf-8")
    assert "CAD And RMS On One Identity" in scenario_text
    assert "Open master-person detail" in scenario_text
    assert "Call/Report Number" in scenario_text
    assert "Master Person" in scenario_text

    from etl_identity_engine.demo_shell.models import GoldenPersonActivity

    golden_id = GoldenPersonActivity.objects.order_by("golden_id").values_list("golden_id", flat=True).first()
    assert golden_id

    golden_response = client.get(f"/golden/{golden_id}/")
    assert golden_response.status_code == 200
    golden_text = golden_response.content.decode("utf-8")
    assert "Call/Report Number" in golden_text
    assert "Master Person" in golden_text

    artifact_response = client.get("/artifacts/data/public_safety_demo/public_safety_demo_summary.json")
    assert artifact_response.status_code == 200
    summary = json.loads(b"".join(artifact_response.streaming_content).decode("utf-8"))
    assert summary["incident_count"] == 12
    assert summary["cross_system_golden_person_count"] == 4


def test_prepare_public_safety_demo_shell_from_persisted_state(tmp_path_factory) -> None:
    workspace = tmp_path_factory.mktemp("public-safety-demo-django-state")
    fixture_source = Path(__file__).resolve().parents[1] / "fixtures" / "public_safety_onboarding"
    fixture_root = workspace / "public-safety-onboarding"
    shutil.copytree(fixture_source, fixture_root)
    manifest_path = fixture_root / "example_manifest.yml"
    state_db = workspace / "state" / "pipeline_state.sqlite"
    base_dir = workspace / "run"

    from etl_identity_engine.cli import main
    from etl_identity_engine.storage.sqlite_store import SQLitePipelineStore

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--manifest",
                str(manifest_path),
                "--state-db",
                str(state_db),
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(state_db)
    run_id = store.latest_completed_run_id()
    assert run_id is not None

    prepared = RUN_MODULE.prepare_public_safety_demo_shell_workspace(
        output_dir=workspace / "shell-from-state",
        bundle=None,
        state_db=str(state_db),
        run_id=run_id,
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version="0.6.0",
        host="127.0.0.1",
        port=8043,
    )

    assert prepared.source_kind == "persisted_state"
    assert prepared.source_run_id == run_id
    assert (prepared.bundle_root / "data" / "public_safety_demo" / "public_safety_demo_summary.json").exists()
    assert (prepared.bundle_root / "persisted_run_source.json").exists()

    with sqlite3.connect(prepared.db_path) as connection:
        demo_run_row = connection.execute(
            "select bundle_name, incident_count, cross_system_golden_person_count from demo_shell_demorun"
        ).fetchone()
        assert demo_run_row[0] == f"persisted-run-{run_id}"
        assert demo_run_row[1] >= 1
        assert demo_run_row[2] >= 1

    from django.test import Client

    client = Client()
    overview_response = client.get("/")
    assert overview_response.status_code == 200
    overview_text = overview_response.content.decode("utf-8")
    assert run_id in overview_text
    assert "ID Network Buyer Demo" in overview_text

    artifact_response = client.get("/artifacts/data/public_safety_demo/public_safety_demo_summary.json")
    assert artifact_response.status_code == 200
    summary = json.loads(b"".join(artifact_response.streaming_content).decode("utf-8"))
    assert summary["source_run_id"] == run_id
    assert summary["source_mode"] == "persisted_state"
