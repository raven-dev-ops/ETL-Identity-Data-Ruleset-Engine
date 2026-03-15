from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "patch_upgrade_customer_pilot.py"
SPEC = importlib.util.spec_from_file_location("patch_upgrade_customer_pilot_script", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _write_bundle_root(root: Path, *, readme_text: str) -> None:
    (root / "runtime").mkdir(parents=True, exist_ok=True)
    (root / "tools").mkdir(parents=True, exist_ok=True)
    (root / "demo_shell").mkdir(parents=True, exist_ok=True)
    (root / "state").mkdir(parents=True, exist_ok=True)
    (root / "pilot_manifest.json").write_text(
        json.dumps({"pilot_name": "public-safety-regressions", "version": "1.0.0"}) + "\n",
        encoding="utf-8",
    )
    (root / "README.md").write_text(readme_text, encoding="utf-8")


def test_patch_upgrade_customer_pilot_preserves_runtime_state(tmp_path: Path, monkeypatch) -> None:
    install_root = tmp_path / "install"
    source_root = tmp_path / "source"
    _write_bundle_root(install_root, readme_text="old install\n")
    _write_bundle_root(source_root, readme_text="new install\n")
    (install_root / "runtime" / "pilot_bootstrap.json").write_text(
        json.dumps({"state_db": "sqlite:///current.sqlite", "run_id": "RUN-CURRENT-001"}) + "\n",
        encoding="utf-8",
    )
    (install_root / "runtime" / "pilot_runtime.env").write_text("ETL_IDENTITY_STATE_DB=sqlite:///current.sqlite\n", encoding="utf-8")
    (install_root / "runtime" / "logs").mkdir(parents=True, exist_ok=True)
    (install_root / "runtime" / "logs" / "service.log").write_text("old log\n", encoding="utf-8")

    rebuild_calls: list[tuple[Path, str]] = []
    service_actions: list[str] = []

    monkeypatch.setattr(MODULE, "_install_runtime", lambda install_root, python_executable: None)
    monkeypatch.setattr(
        MODULE,
        "_rebuild_demo_shell_from_current_state",
        lambda install_root, python_executable: rebuild_calls.append((install_root, python_executable)),
    )
    monkeypatch.setattr(
        MODULE,
        "_service_status_snapshot",
        lambda install_root: {"demo_shell": {"installed": True, "status": "running"}},
    )
    monkeypatch.setattr(
        MODULE,
        "_manage_services",
        lambda install_root, action, service_kind="all": service_actions.append(action),
    )

    summary = MODULE.patch_upgrade_customer_pilot(
        install_root=install_root,
        source_root=source_root,
        mode="preserve_state",
        python_executable="python",
    )

    assert summary["mode"] == "preserve_state"
    assert service_actions == ["stop", "start"]
    assert rebuild_calls == [(install_root, "python")]
    assert (install_root / "README.md").read_text(encoding="utf-8") == "new install\n"
    assert json.loads((install_root / "runtime" / "pilot_bootstrap.json").read_text(encoding="utf-8"))["run_id"] == "RUN-CURRENT-001"
    assert (install_root / "runtime" / "logs" / "service.log").read_text(encoding="utf-8") == "old log\n"


def test_patch_upgrade_customer_pilot_reseeds_with_existing_runtime_settings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    install_root = tmp_path / "install"
    source_root = tmp_path / "source"
    _write_bundle_root(install_root, readme_text="old install\n")
    _write_bundle_root(source_root, readme_text="new install\n")
    (install_root / "runtime" / "pilot_bootstrap.json").write_text(
        json.dumps(
            {
                "postgres_port": 55432,
                "postgres_container_name": "etl-identity-pilot-public-safety-regressions",
                "postgres_db": "identity_state",
                "postgres_user": "etl_identity",
                "postgres_password": "pilot-password",
                "demo_host": "127.0.0.1",
                "demo_port": 8001,
                "service_port": 8011,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    reseed_calls: list[tuple[Path, str, dict[str, object] | None]] = []
    monkeypatch.setattr(MODULE, "_install_runtime", lambda install_root, python_executable: None)
    monkeypatch.setattr(
        MODULE,
        "_reseed_install",
        lambda install_root, python_executable, existing_config: reseed_calls.append(
            (install_root, python_executable, existing_config)
        ),
    )
    monkeypatch.setattr(MODULE, "_service_status_snapshot", lambda install_root: {})

    summary = MODULE.patch_upgrade_customer_pilot(
        install_root=install_root,
        source_root=source_root,
        mode="reseed",
        python_executable="python",
    )

    assert summary["mode"] == "reseed"
    assert len(reseed_calls) == 1
    assert reseed_calls[0][2]["service_port"] == 8011
    assert (install_root / "README.md").read_text(encoding="utf-8") == "new install\n"


def test_patch_upgrade_main_accepts_source_bundle_zip(tmp_path: Path, monkeypatch) -> None:
    install_root = tmp_path / "install"
    source_root = tmp_path / "source"
    _write_bundle_root(install_root, readme_text="old install\n")
    _write_bundle_root(source_root, readme_text="new install\n")
    bundle_zip = tmp_path / "upgrade.zip"
    with zipfile.ZipFile(bundle_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in source_root.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(source_root).as_posix())

    captured: dict[str, object] = {}

    def fake_patch_upgrade(*, install_root: Path, source_root: Path, mode: str, python_executable: str):
        captured["install_root"] = install_root
        captured["source_root_has_manifest"] = (source_root / "pilot_manifest.json").exists()
        captured["mode"] = mode
        return {"status": "ok"}

    monkeypatch.setattr(MODULE, "patch_upgrade_customer_pilot", fake_patch_upgrade)

    assert (
        MODULE.main(
            [
                "--install-root",
                str(install_root),
                "--source-bundle",
                str(bundle_zip),
                "--mode",
                "preserve_state",
            ]
        )
        == 0
    )
    assert captured["install_root"] == install_root
    assert captured["source_root_has_manifest"] is True
    assert captured["mode"] == "preserve_state"
