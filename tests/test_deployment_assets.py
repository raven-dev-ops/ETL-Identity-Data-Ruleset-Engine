from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_dockerfile_exposes_cli_entrypoint_and_service_port() -> None:
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert 'FROM python:3.11-slim' in dockerfile
    assert 'ENTRYPOINT ["etl-identity-engine"]' in dockerfile
    assert "EXPOSE 8000" in dockerfile


def test_compose_manifest_defines_batch_and_service_topology() -> None:
    compose = yaml.safe_load((REPO_ROOT / "deploy" / "compose.yaml").read_text(encoding="utf-8"))

    services = compose["services"]
    assert set(services) == {"identity-batch", "identity-service"}
    assert services["identity-batch"]["command"][:3] == ["run-all", "--environment", "container"]
    assert services["identity-service"]["command"][:3] == ["serve-api", "--environment", "container"]
    assert services["identity-service"]["healthcheck"]["test"][0] == "CMD"
    assert services["identity-service"]["ports"] == ["${ETL_IDENTITY_SERVICE_PORT:-8000}:8000"]


def test_container_env_example_documents_required_values() -> None:
    env_text = (REPO_ROOT / "deploy" / "container.env.example").read_text(encoding="utf-8")

    assert "ETL_IDENTITY_IMAGE=" in env_text
    assert "ETL_IDENTITY_RUNTIME_ROOT=" in env_text
    assert "ETL_IDENTITY_SERVICE_READER_API_KEY=" in env_text
    assert "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY=" in env_text


def test_container_smoke_script_targets_compose_topology() -> None:
    script_text = (REPO_ROOT / "scripts" / "container_smoke_test.py").read_text(encoding="utf-8")

    assert '"docker", "build"' in script_text
    assert "identity-batch" in script_text
    assert "identity-service" in script_text
    assert "/healthz" in script_text
