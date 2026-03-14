from __future__ import annotations

import argparse
import csv
import http.client
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import yaml

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS


REPO_ROOT = Path(__file__).resolve().parents[1]
KUBERNETES_ROOT = REPO_ROOT / "deploy" / "kubernetes"


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=check,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )


def _load_yaml(path: Path) -> dict[str, object]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return loaded


def _require_mapping(value: object, *, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be a mapping")
    return value


def _require_list(value: object, *, context: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must be a list")
    return value


def _write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet_rows(path: Path, rows: list[dict[str, str]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _person_row(
    *,
    source_record_id: str,
    person_entity_id: str,
    source_system: str,
    first_name: str,
    last_name: str,
    dob: str,
    address: str,
    phone: str,
) -> dict[str, str]:
    return {
        "source_record_id": source_record_id,
        "person_entity_id": person_entity_id,
        "source_system": source_system,
        "first_name": first_name,
        "last_name": last_name,
        "dob": dob,
        "address": address,
        "city": "Columbus",
        "state": "OH",
        "postal_code": "43004",
        "phone": phone,
        "updated_at": "2025-01-01T00:00:00Z",
        "is_conflict_variant": "false",
        "conflict_types": "",
    }


def _write_manifest(path: Path) -> None:
    required_columns = "\n".join(f"        - {column}" for column in PERSON_HEADERS)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""
manifest_version: "1.0"
entity_type: person
batch_id: kubernetes-smoke-001
landing_zone:
  kind: local_filesystem
  base_path: .
sources:
  - source_id: source_a
    path: agency_a.csv
    format: csv
    schema_version: person-v1
    required_columns:
{required_columns}
  - source_id: source_b
    path: agency_b.parquet
    format: parquet
    schema_version: person-v1
    required_columns:
{required_columns}
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _prepare_landing_zone(runtime_root: Path) -> None:
    landing_dir = runtime_root / "landing"
    _write_manifest(landing_dir / "batch-manifest.yaml")
    _write_csv_rows(
        landing_dir / "agency_a.csv",
        [
            _person_row(
                source_record_id="A-1",
                person_entity_id="P-1",
                source_system="source_a",
                first_name="John",
                last_name="Smith",
                dob="1985-03-12",
                address="123 Main St",
                phone="5551111111",
            )
        ],
    )
    _write_parquet_rows(
        landing_dir / "agency_b.parquet",
        [
            _person_row(
                source_record_id="B-1",
                person_entity_id="P-2",
                source_system="source_b",
                first_name="Jon",
                last_name="Smith",
                dob="1985-03-12",
                address="123 Main St",
                phone="5551111111",
            )
        ],
    )


def _validate_manifests() -> dict[str, list[str]]:
    kustomization = _load_yaml(KUBERNETES_ROOT / "kustomization.yaml")
    resources = tuple(
        str(item)
        for item in _require_list(kustomization.get("resources"), context="kustomization.resources")
    )
    expected_resources = {
        "namespace.yaml",
        "landing-pvc.yaml",
        "postgres-service.yaml",
        "postgres-statefulset.yaml",
        "service-service.yaml",
        "service-deployment.yaml",
    }
    if set(resources) != expected_resources:
        raise ValueError(
            "deploy/kubernetes/kustomization.yaml resources do not match the supported cluster base"
        )

    postgres_secret = _load_yaml(KUBERNETES_ROOT / "postgres-secret.example.yaml")
    postgres_secret_data = _require_mapping(
        postgres_secret.get("stringData"),
        context="postgres-secret.example.yaml.stringData",
    )
    if set(postgres_secret_data) != {"POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"}:
        raise ValueError("postgres-secret.example.yaml must declare the PostgreSQL bootstrap keys")

    runtime_secret = _load_yaml(KUBERNETES_ROOT / "runtime-secret.example.yaml")
    runtime_secret_data = _require_mapping(
        runtime_secret.get("stringData"),
        context="runtime-secret.example.yaml.stringData",
    )
    required_runtime_keys = {
        "ETL_IDENTITY_STATE_DB",
        "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY",
        "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY",
        "ETL_IDENTITY_SERVICE_READER_API_KEY",
        "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY",
    }
    if set(runtime_secret_data) != required_runtime_keys:
        raise ValueError("runtime-secret.example.yaml must declare the runtime env keys")

    deployment = _load_yaml(KUBERNETES_ROOT / "service-deployment.yaml")
    deployment_spec = _require_mapping(deployment.get("spec"), context="service deployment spec")
    template = _require_mapping(deployment_spec.get("template"), context="service deployment template")
    pod_spec = _require_mapping(template.get("spec"), context="service pod spec")
    container = _require_list(pod_spec.get("containers"), context="service containers")[0]
    container_mapping = _require_mapping(container, context="service container")
    deployment_args = [str(item) for item in _require_list(container_mapping.get("args"), context="service args")]
    if deployment_args[:3] != ["serve-api", "--environment", "cluster"]:
        raise ValueError("service deployment must launch serve-api in the cluster environment")

    env_from = _require_list(container_mapping.get("envFrom"), context="service envFrom")
    secret_ref = _require_mapping(env_from[0], context="service envFrom[0]")
    secret_mapping = _require_mapping(secret_ref.get("secretRef"), context="service secretRef")
    if secret_mapping.get("name") != "identity-runtime-secret":
        raise ValueError("service deployment must read runtime settings from identity-runtime-secret")

    upgrade_job = _load_yaml(KUBERNETES_ROOT / "state-db-upgrade-job.yaml")
    upgrade_args = _extract_job_args(upgrade_job, "state-db-upgrade job")
    if upgrade_args[:3] != ["state-db-upgrade", "--environment", "cluster"]:
        raise ValueError("state-db-upgrade job must target the cluster environment")

    batch_job = _load_yaml(KUBERNETES_ROOT / "batch-job.yaml")
    batch_args = _extract_job_args(batch_job, "batch job")
    expected_batch_prefix = [
        "run-all",
        "--environment",
        "cluster",
        "--base-dir",
        "/runtime/output",
        "--manifest",
        "/runtime/landing/batch-manifest.yaml",
    ]
    if batch_args[: len(expected_batch_prefix)] != expected_batch_prefix:
        raise ValueError("batch job must target the manifest-driven cluster path")

    ingress = _load_yaml(KUBERNETES_ROOT / "service-ingress.example.yaml")
    ingress_spec = _require_mapping(ingress.get("spec"), context="ingress spec")
    ingress_rules = _require_list(ingress_spec.get("rules"), context="ingress rules")
    ingress_rule = _require_mapping(ingress_rules[0], context="ingress rule")
    ingress_http = _require_mapping(ingress_rule.get("http"), context="ingress http")
    ingress_paths = _require_list(ingress_http.get("paths"), context="ingress paths")
    ingress_path = _require_mapping(ingress_paths[0], context="ingress path")
    backend = _require_mapping(ingress_path.get("backend"), context="ingress backend")
    service_backend = _require_mapping(backend.get("service"), context="ingress backend service")
    if service_backend.get("name") != "identity-service":
        raise ValueError("ingress example must route to identity-service")

    return {
        "upgrade_args": upgrade_args,
        "batch_args": batch_args,
        "service_args": deployment_args,
    }


def _extract_job_args(job: dict[str, object], context: str) -> list[str]:
    spec = _require_mapping(job.get("spec"), context=f"{context} spec")
    template = _require_mapping(spec.get("template"), context=f"{context} template")
    pod_spec = _require_mapping(template.get("spec"), context=f"{context} pod spec")
    container = _require_list(pod_spec.get("containers"), context=f"{context} containers")[0]
    container_mapping = _require_mapping(container, context=f"{context} container")
    return [str(item) for item in _require_list(container_mapping.get("args"), context=f"{context} args")]


def _wait_for_postgres(container_name: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = _run(
            ["docker", "exec", container_name, "pg_isready", "-U", "etl_identity", "-d", "identity_state"],
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("Timed out waiting for PostgreSQL container readiness")


def _wait_for_service_health(port: int, reader_api_key: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/healthz",
        headers={"X-API-Key": reader_api_key},
    )
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, http.client.RemoteDisconnected, TimeoutError):
            time.sleep(1)
            continue
    raise RuntimeError(f"Timed out waiting for service health on port {port}")


def _docker_env_args(env_map: dict[str, str]) -> list[str]:
    args: list[str] = []
    for key, value in env_map.items():
        args.extend(["-e", f"{key}={value}"])
    return args


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-tag", default="etl-identity-engine:kubernetes-smoke")
    parser.add_argument("--service-port", default=18081, type=int)
    parser.add_argument("--reader-api-key", default="cluster-reader-secret")
    parser.add_argument("--operator-api-key", default="cluster-operator-secret")
    args = parser.parse_args(argv)

    commands = _validate_manifests()

    runtime_root = Path(tempfile.mkdtemp(prefix="etl-identity-engine-kubernetes-smoke-"))
    _prepare_landing_zone(runtime_root)
    network_name = f"etl-identity-k8s-smoke-{int(time.time())}"
    postgres_container_name = f"{network_name}-postgres"
    service_container_name = f"{network_name}-service"

    postgres_env = {
        "POSTGRES_DB": "identity_state",
        "POSTGRES_USER": "etl_identity",
        "POSTGRES_PASSWORD": "cluster-smoke-password",
    }
    runtime_env = {
        "ETL_IDENTITY_STATE_DB": (
            "postgresql://etl_identity:cluster-smoke-password@identity-postgres:5432/identity_state"
        ),
        "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY": "disabled",
        "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY": "disabled",
        "ETL_IDENTITY_SERVICE_READER_API_KEY": args.reader_api_key,
        "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY": args.operator_api_key,
    }
    try:
        print(f"building image {args.image_tag}")
        _run(["docker", "build", "-t", args.image_tag, "."])

        print("creating smoke network")
        _run(["docker", "network", "create", network_name])

        print("starting PostgreSQL container")
        _run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                postgres_container_name,
                "--network",
                network_name,
                "--network-alias",
                "identity-postgres",
                *_docker_env_args(postgres_env),
                "postgres:16-alpine",
            ]
        )
        _wait_for_postgres(postgres_container_name, timeout_seconds=60)

        print("running state-db upgrade job command")
        _run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                network_name,
                *_docker_env_args(runtime_env),
                args.image_tag,
                *commands["upgrade_args"],
            ]
        )

        print("running manifest-driven batch job command")
        _run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                network_name,
                "-v",
                f"{runtime_root.as_posix()}:/runtime",
                *_docker_env_args(runtime_env),
                args.image_tag,
                *commands["batch_args"],
            ]
        )

        golden_records = runtime_root / "output" / "data" / "golden" / "golden_person_records.csv"
        if not golden_records.exists():
            raise RuntimeError(f"Expected golden records were not created: {golden_records}")

        print("starting service deployment command")
        _run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                service_container_name,
                "--network",
                network_name,
                "-p",
                f"{args.service_port}:8000",
                *_docker_env_args(runtime_env),
                args.image_tag,
                *commands["service_args"],
            ]
        )

        _wait_for_service_health(args.service_port, args.reader_api_key, timeout_seconds=60)

        metrics_request = urllib.request.Request(
            f"http://127.0.0.1:{args.service_port}/api/v1/metrics",
            headers={"X-API-Key": args.reader_api_key},
        )
        with urllib.request.urlopen(metrics_request, timeout=5) as response:
            if response.status != 200:
                raise RuntimeError("Kubernetes smoke service metrics endpoint did not return 200")

        latest_run_request = urllib.request.Request(
            f"http://127.0.0.1:{args.service_port}/api/v1/runs/latest",
            headers={"X-API-Key": args.reader_api_key},
        )
        with urllib.request.urlopen(latest_run_request, timeout=5) as response:
            if response.status != 200:
                raise RuntimeError("Kubernetes smoke service latest-run endpoint did not return 200")

        print("kubernetes deployment smoke test passed")
        return 0
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stdout)
        sys.stderr.write(exc.stderr)
        raise
    except Exception:
        postgres_logs = _run(["docker", "logs", postgres_container_name], check=False)
        service_logs = _run(["docker", "logs", service_container_name], check=False)
        sys.stderr.write(postgres_logs.stdout)
        sys.stderr.write(postgres_logs.stderr)
        sys.stderr.write(service_logs.stdout)
        sys.stderr.write(service_logs.stderr)
        raise
    finally:
        _run(["docker", "rm", "-f", service_container_name], check=False)
        _run(["docker", "rm", "-f", postgres_container_name], check=False)
        _run(["docker", "network", "rm", network_name], check=False)
        shutil.rmtree(runtime_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
