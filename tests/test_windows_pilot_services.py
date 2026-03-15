from __future__ import annotations

import json
from pathlib import Path

from etl_identity_engine.windows_pilot_services import (
    WindowsPilotServiceDefinition,
    WindowsPilotServiceStatus,
    load_windows_pilot_service_definitions,
    manage_windows_pilot_services,
)


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
