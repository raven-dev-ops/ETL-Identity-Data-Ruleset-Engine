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
    assert "ETL_IDENTITY_SERVICE_READER_TENANT_ID=" in env_text
    assert "ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID=" in env_text


def test_cjis_env_example_documents_required_values() -> None:
    env_text = (REPO_ROOT / "deploy" / "cjis.env.example").read_text(encoding="utf-8")

    assert "ETL_IDENTITY_STATE_DB=" in env_text
    assert "ETL_IDENTITY_SERVICE_JWT_ISSUER=" in env_text
    assert "ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM_FILE=" in env_text
    assert "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY_FILE=" in env_text
    assert "ETL_IDENTITY_RUNTIME_AUTH_MAX_AGE_HOURS=" in env_text
    assert "ETL_IDENTITY_TLS_CERT_PATH=" in env_text
    assert "ETL_IDENTITY_CJIS_MFA_ENFORCED=1" in env_text


def test_container_smoke_script_targets_compose_topology() -> None:
    script_text = (REPO_ROOT / "scripts" / "container_smoke_test.py").read_text(encoding="utf-8")

    assert '"docker", "build"' in script_text
    assert "identity-batch" in script_text
    assert "identity-service" in script_text
    assert "/healthz" in script_text


def test_kubernetes_kustomization_defines_cluster_base_resources() -> None:
    kustomization = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes" / "kustomization.yaml").read_text(encoding="utf-8")
    )

    assert kustomization["namespace"] == "etl-identity"
    assert set(kustomization["resources"]) == {
        "namespace.yaml",
        "landing-pvc.yaml",
        "postgres-service.yaml",
        "postgres-statefulset.yaml",
        "service-service.yaml",
        "service-deployment.yaml",
    }


def test_kubernetes_service_and_job_manifests_target_cluster_runtime() -> None:
    service_deployment = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes" / "service-deployment.yaml").read_text(
            encoding="utf-8"
        )
    )
    batch_job = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes" / "batch-job.yaml").read_text(encoding="utf-8")
    )
    upgrade_job = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes" / "state-db-upgrade-job.yaml").read_text(
            encoding="utf-8"
        )
    )

    service_container = service_deployment["spec"]["template"]["spec"]["containers"][0]
    batch_container = batch_job["spec"]["template"]["spec"]["containers"][0]
    upgrade_container = upgrade_job["spec"]["template"]["spec"]["containers"][0]

    assert service_container["args"][:3] == ["serve-api", "--environment", "cluster"]
    assert batch_container["args"][:7] == [
        "run-all",
        "--environment",
        "cluster",
        "--base-dir",
        "/runtime/output",
        "--manifest",
        "/runtime/landing/batch-manifest.yaml",
    ]
    assert upgrade_container["args"][:3] == ["state-db-upgrade", "--environment", "cluster"]
    assert service_container["envFrom"][0]["secretRef"]["name"] == "identity-runtime-secret"
    assert batch_container["envFrom"][0]["secretRef"]["name"] == "identity-runtime-secret"
    assert upgrade_container["envFrom"][0]["secretRef"]["name"] == "identity-runtime-secret"


def test_kubernetes_example_secrets_document_required_keys() -> None:
    postgres_secret = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes" / "postgres-secret.example.yaml").read_text(
            encoding="utf-8"
        )
    )
    runtime_secret = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes" / "runtime-secret.example.yaml").read_text(
            encoding="utf-8"
        )
    )
    ingress = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes" / "service-ingress.example.yaml").read_text(
            encoding="utf-8"
        )
    )

    assert set(postgres_secret["stringData"]) == {"POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"}
    assert set(runtime_secret["stringData"]) == {
        "ETL_IDENTITY_STATE_DB",
        "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY",
        "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY",
        "ETL_IDENTITY_SERVICE_READER_API_KEY",
        "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY",
        "ETL_IDENTITY_SERVICE_READER_TENANT_ID",
        "ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID",
    }
    assert ingress["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"] == (
        "identity-service"
    )


def test_kubernetes_cluster_runtime_environment_exists() -> None:
    runtime_config = yaml.safe_load(
        (REPO_ROOT / "config" / "runtime_environments.yml").read_text(encoding="utf-8")
    )

    cluster = runtime_config["environments"]["cluster"]
    assert cluster["state_db"] == "${ETL_IDENTITY_STATE_DB}"
    assert cluster["service_auth"]["mode"] == "api_key"
    assert cluster["service_auth"]["reader_api_key"] == "${ETL_IDENTITY_SERVICE_READER_API_KEY}"
    assert cluster["service_auth"]["operator_api_key"] == "${ETL_IDENTITY_SERVICE_OPERATOR_API_KEY}"
    assert cluster["service_auth"]["reader_tenant_id"] == "${ETL_IDENTITY_SERVICE_READER_TENANT_ID:-default}"
    assert cluster["service_auth"]["operator_tenant_id"] == "${ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID:-default}"


def test_cjis_runtime_environment_exists() -> None:
    runtime_config = yaml.safe_load(
        (REPO_ROOT / "config" / "runtime_environments.yml").read_text(encoding="utf-8")
    )

    cjis = runtime_config["environments"]["cjis"]
    assert cjis["state_db"] == "${ETL_IDENTITY_STATE_DB}"
    assert cjis["service_auth"]["mode"] == "jwt"
    assert cjis["service_auth"]["algorithms"] == ["RS256"]
    assert cjis["service_auth"]["jwt_public_key_pem"] == "${ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM}"
    assert cjis["service_auth"]["tenant_claim_path"] == "tenant_id"


def test_kubernetes_smoke_script_targets_postgresql_backed_cluster_topology() -> None:
    script_text = (REPO_ROOT / "scripts" / "kubernetes_manifest_smoke.py").read_text(
        encoding="utf-8"
    )

    expected_fragments = (
        '"docker", "build"',
        '"docker", "network", "create"',
        "postgres:16-alpine",
        "state-db-upgrade",
        "/runtime/landing/batch-manifest.yaml",
        "/api/v1/runs/latest",
        "identity-postgres",
        "ETL_IDENTITY_SERVICE_READER_TENANT_ID",
    )

    for fragment in expected_fragments:
        assert fragment in script_text


def test_kubernetes_ha_kustomization_defines_external_writer_app_resources() -> None:
    kustomization = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes-ha" / "kustomization.yaml").read_text(encoding="utf-8")
    )

    assert kustomization["namespace"] == "etl-identity"
    assert set(kustomization["resources"]) == {
        "../kubernetes/namespace.yaml",
        "../kubernetes/landing-pvc.yaml",
        "../kubernetes/service-service.yaml",
        "pod-disruption-budget.yaml",
        "service-deployment.yaml",
    }


def test_kubernetes_ha_service_and_job_manifests_target_cluster_ha_runtime() -> None:
    service_deployment = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes-ha" / "service-deployment.yaml").read_text(
            encoding="utf-8"
        )
    )
    batch_job = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes-ha" / "batch-job.yaml").read_text(encoding="utf-8")
    )
    upgrade_job = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes-ha" / "state-db-upgrade-job.yaml").read_text(
            encoding="utf-8"
        )
    )
    disruption_budget = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes-ha" / "pod-disruption-budget.yaml").read_text(
            encoding="utf-8"
        )
    )

    service_container = service_deployment["spec"]["template"]["spec"]["containers"][0]
    batch_container = batch_job["spec"]["template"]["spec"]["containers"][0]
    upgrade_container = upgrade_job["spec"]["template"]["spec"]["containers"][0]
    anti_affinity = service_deployment["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"]

    assert service_deployment["spec"]["replicas"] == 2
    assert service_container["args"][:3] == ["serve-api", "--environment", "cluster_ha"]
    assert batch_container["args"][:7] == [
        "run-all",
        "--environment",
        "cluster_ha",
        "--base-dir",
        "/runtime/output",
        "--manifest",
        "/runtime/landing/batch-manifest.yaml",
    ]
    assert upgrade_container["args"][:3] == ["state-db-upgrade", "--environment", "cluster_ha"]
    assert service_container["envFrom"][0]["secretRef"]["name"] == "identity-ha-runtime-secret"
    assert batch_container["envFrom"][0]["secretRef"]["name"] == "identity-ha-runtime-secret"
    assert upgrade_container["envFrom"][0]["secretRef"]["name"] == "identity-ha-runtime-secret"
    assert anti_affinity["requiredDuringSchedulingIgnoredDuringExecution"]
    assert disruption_budget["spec"]["minAvailable"] == 1


def test_kubernetes_ha_example_assets_document_writer_endpoint_contract() -> None:
    runtime_secret = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes-ha" / "runtime-secret.example.yaml").read_text(
            encoding="utf-8"
        )
    )
    external_writer_service = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kubernetes-ha" / "external-writer-service.example.yaml").read_text(
            encoding="utf-8"
        )
    )

    assert set(runtime_secret["stringData"]) == {
        "ETL_IDENTITY_STATE_DB",
        "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY",
        "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY",
        "ETL_IDENTITY_SERVICE_READER_API_KEY",
        "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY",
        "ETL_IDENTITY_SERVICE_READER_TENANT_ID",
        "ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID",
    }
    assert "identity-postgres-rw" in runtime_secret["stringData"]["ETL_IDENTITY_STATE_DB"]
    assert "target_session_attrs=read-write" in runtime_secret["stringData"]["ETL_IDENTITY_STATE_DB"]
    assert external_writer_service["spec"]["type"] == "ExternalName"
    assert external_writer_service["metadata"]["name"] == "identity-postgres-rw"


def test_kubernetes_ha_runtime_environment_exists() -> None:
    runtime_config = yaml.safe_load(
        (REPO_ROOT / "config" / "runtime_environments.yml").read_text(encoding="utf-8")
    )

    cluster_ha = runtime_config["environments"]["cluster_ha"]
    assert cluster_ha["state_db"] == "${ETL_IDENTITY_STATE_DB}"
    assert cluster_ha["service_auth"]["mode"] == "api_key"
    assert cluster_ha["service_auth"]["reader_api_key"] == "${ETL_IDENTITY_SERVICE_READER_API_KEY}"
    assert cluster_ha["service_auth"]["operator_api_key"] == "${ETL_IDENTITY_SERVICE_OPERATOR_API_KEY}"
    assert cluster_ha["service_auth"]["reader_tenant_id"] == "${ETL_IDENTITY_SERVICE_READER_TENANT_ID:-default}"
    assert cluster_ha["service_auth"]["operator_tenant_id"] == "${ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID:-default}"


def test_postgresql_ha_rehearsal_script_targets_failover_and_restore_validation() -> None:
    script_text = (REPO_ROOT / "scripts" / "postgresql_ha_rehearsal.py").read_text(encoding="utf-8")

    expected_fragments = (
        '"docker", "build"',
        '"docker", "volume", "create"',
        "target_session_attrs=read-write",
        "identity-postgres-rw",
        "verify-replay-bundle",
        "backup-state-bundle",
        "restore-state-bundle",
        "replay-run",
        "/api/v1/runs/latest",
        "service_reconnected_after_writer_failover",
    )

    for fragment in expected_fragments:
        assert fragment in script_text
