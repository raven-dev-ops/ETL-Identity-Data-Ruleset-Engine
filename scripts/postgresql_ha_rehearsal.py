from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any
import urllib.error
import urllib.request

import yaml

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.storage.sqlite_store import SQLitePipelineStore


REPO_ROOT = Path(__file__).resolve().parents[1]
KUBERNETES_HA_ROOT = REPO_ROOT / "deploy" / "kubernetes-ha"
PRIMARY_WRITER_ALIAS = "identity-postgres-rw"
RESTORE_WRITER_ALIAS = "identity-postgres-restore"
PRIMARY_RUNTIME_SECRET = "identity-ha-runtime-secret"
PRIMARY_DB_NAME = "identity_state"
PRIMARY_DB_USER = "etl_identity"
PRIMARY_DB_PASSWORD = "ha-smoke-password"
RESTORE_DB_PASSWORD = "ha-restore-password"


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


def _run_local_cli(argv: list[str], *, expect_json: bool = False) -> Any:
    completed = _run([sys.executable, "-m", "etl_identity_engine.cli", *argv], check=False)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise RuntimeError(
            f"command failed ({completed.returncode}): python -m etl_identity_engine.cli {' '.join(argv)}\n{detail}"
        )
    if not expect_json:
        return completed.stdout
    return json.loads(completed.stdout)


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
batch_id: postgresql-ha-rehearsal-001
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


def _set_world_access(path: Path) -> None:
    if path.is_dir():
        os.chmod(path, 0o777)
    else:
        os.chmod(path, 0o666)


def _ensure_runtime_root_permissions(runtime_root: Path) -> None:
    for directory in (
        runtime_root,
        runtime_root / "landing",
        runtime_root / "output",
        runtime_root / "recovery",
        runtime_root / "replayed-run",
    ):
        directory.mkdir(parents=True, exist_ok=True)
        _set_world_access(directory)


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
    for path in (
        landing_dir / "batch-manifest.yaml",
        landing_dir / "agency_a.csv",
        landing_dir / "agency_b.parquet",
    ):
        _set_world_access(path)


def _extract_job_args(job: dict[str, object], context: str) -> list[str]:
    spec = _require_mapping(job.get("spec"), context=f"{context} spec")
    template = _require_mapping(spec.get("template"), context=f"{context} template")
    pod_spec = _require_mapping(template.get("spec"), context=f"{context} pod spec")
    container = _require_list(pod_spec.get("containers"), context=f"{context} containers")[0]
    container_mapping = _require_mapping(container, context=f"{context} container")
    return [str(item) for item in _require_list(container_mapping.get("args"), context=f"{context} args")]


def _validate_manifests() -> dict[str, list[str]]:
    kustomization = _load_yaml(KUBERNETES_HA_ROOT / "kustomization.yaml")
    resources = tuple(
        str(item)
        for item in _require_list(kustomization.get("resources"), context="kubernetes-ha.kustomization.resources")
    )
    expected_resources = {
        "../kubernetes/namespace.yaml",
        "../kubernetes/landing-pvc.yaml",
        "../kubernetes/service-service.yaml",
        "pod-disruption-budget.yaml",
        "service-deployment.yaml",
    }
    if set(resources) != expected_resources:
        raise ValueError("deploy/kubernetes-ha/kustomization.yaml resources do not match the supported HA base")

    runtime_secret = _load_yaml(KUBERNETES_HA_ROOT / "runtime-secret.example.yaml")
    runtime_secret_data = _require_mapping(
        runtime_secret.get("stringData"),
        context="kubernetes-ha.runtime-secret.example.yaml.stringData",
    )
    required_runtime_keys = {
        "ETL_IDENTITY_STATE_DB",
        "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY",
        "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY",
        "ETL_IDENTITY_SERVICE_READER_API_KEY",
        "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY",
        "ETL_IDENTITY_SERVICE_READER_TENANT_ID",
        "ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID",
    }
    if set(runtime_secret_data) != required_runtime_keys:
        raise ValueError("runtime-secret.example.yaml must declare the HA runtime env keys")
    state_db_value = str(runtime_secret_data["ETL_IDENTITY_STATE_DB"])
    if PRIMARY_WRITER_ALIAS not in state_db_value or "target_session_attrs=read-write" not in state_db_value:
        raise ValueError("HA runtime secret must point to the writer endpoint with target_session_attrs=read-write")

    deployment = _load_yaml(KUBERNETES_HA_ROOT / "service-deployment.yaml")
    deployment_spec = _require_mapping(deployment.get("spec"), context="ha service deployment spec")
    if deployment_spec.get("replicas") != 2:
        raise ValueError("HA service deployment must run two replicas")
    template = _require_mapping(deployment_spec.get("template"), context="ha service deployment template")
    pod_spec = _require_mapping(template.get("spec"), context="ha service pod spec")
    affinity = _require_mapping(pod_spec.get("affinity"), context="ha service affinity")
    pod_anti_affinity = _require_mapping(
        affinity.get("podAntiAffinity"),
        context="ha service podAntiAffinity",
    )
    _require_list(
        pod_anti_affinity.get("requiredDuringSchedulingIgnoredDuringExecution"),
        context="ha service required anti-affinity rules",
    )
    container = _require_list(pod_spec.get("containers"), context="ha service containers")[0]
    container_mapping = _require_mapping(container, context="ha service container")
    service_args = [
        str(item) for item in _require_list(container_mapping.get("args"), context="ha service args")
    ]
    if service_args[:3] != ["serve-api", "--environment", "cluster_ha"]:
        raise ValueError("HA service deployment must launch serve-api in the cluster_ha environment")
    env_from = _require_list(container_mapping.get("envFrom"), context="ha service envFrom")
    secret_ref = _require_mapping(env_from[0], context="ha service envFrom[0]")
    secret_mapping = _require_mapping(secret_ref.get("secretRef"), context="ha service secretRef")
    if secret_mapping.get("name") != PRIMARY_RUNTIME_SECRET:
        raise ValueError("HA service deployment must read runtime settings from identity-ha-runtime-secret")

    disruption_budget = _load_yaml(KUBERNETES_HA_ROOT / "pod-disruption-budget.yaml")
    disruption_spec = _require_mapping(disruption_budget.get("spec"), context="ha pod disruption budget spec")
    if disruption_spec.get("minAvailable") != 1:
        raise ValueError("HA pod disruption budget must preserve at least one available replica")

    upgrade_job = _load_yaml(KUBERNETES_HA_ROOT / "state-db-upgrade-job.yaml")
    upgrade_args = _extract_job_args(upgrade_job, "ha state-db-upgrade job")
    if upgrade_args[:3] != ["state-db-upgrade", "--environment", "cluster_ha"]:
        raise ValueError("HA state-db-upgrade job must target the cluster_ha environment")

    batch_job = _load_yaml(KUBERNETES_HA_ROOT / "batch-job.yaml")
    batch_args = _extract_job_args(batch_job, "ha batch job")
    expected_batch_prefix = [
        "run-all",
        "--environment",
        "cluster_ha",
        "--base-dir",
        "/runtime/output",
        "--manifest",
        "/runtime/landing/batch-manifest.yaml",
    ]
    if batch_args[: len(expected_batch_prefix)] != expected_batch_prefix:
        raise ValueError("HA batch job must target the manifest-driven cluster_ha path")

    writer_service = _load_yaml(KUBERNETES_HA_ROOT / "external-writer-service.example.yaml")
    writer_service_spec = _require_mapping(
        writer_service.get("spec"),
        context="ha external writer service spec",
    )
    if writer_service_spec.get("type") != "ExternalName":
        raise ValueError("HA external writer service example must use ExternalName")
    if not str(writer_service_spec.get("externalName", "") or "").strip():
        raise ValueError("HA external writer service example must declare a writer DNS name")

    return {
        "upgrade_args": upgrade_args,
        "batch_args": batch_args,
        "service_args": service_args,
    }


def _wait_for_postgres(container_name: str, password: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = _run(
            [
                "docker",
                "exec",
                container_name,
                "env",
                f"PGPASSWORD={password}",
                "pg_isready",
                "-U",
                PRIMARY_DB_USER,
                "-d",
                PRIMARY_DB_NAME,
            ],
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for PostgreSQL readiness in container {container_name}")


def _wait_for_service_endpoint(url: str, reader_api_key: str, timeout_seconds: int) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    request = urllib.request.Request(url, headers={"X-API-Key": reader_api_key})
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                if response.status == 200:
                    return json.loads(response.read().decode("utf-8"))
        except (
            urllib.error.URLError,
            http.client.RemoteDisconnected,
            ConnectionResetError,
            TimeoutError,
            json.JSONDecodeError,
        ):
            time.sleep(1)
            continue
    raise RuntimeError(f"Timed out waiting for service endpoint {url}")


def _docker_env_args(env_map: dict[str, str]) -> list[str]:
    args: list[str] = []
    for key, value in env_map.items():
        args.extend(["-e", f"{key}={value}"])
    return args


def _run_container_command(
    *,
    image_tag: str,
    network_name: str,
    env_map: dict[str, str],
    args: list[str],
    mounts: list[tuple[Path, str]] | None = None,
) -> subprocess.CompletedProcess[str]:
    command = ["docker", "run", "--rm", "--network", network_name]
    if mounts:
        for host_path, container_path in mounts:
            command.extend(["-v", f"{host_path.as_posix()}:{container_path}"])
    command.extend(_docker_env_args(env_map))
    command.append(image_tag)
    command.extend(args)
    return _run(command)


def _start_postgres_writer(
    *,
    container_name: str,
    network_name: str,
    network_alias: str,
    volume_name: str,
    host_port: int,
    password: str,
) -> None:
    postgres_env = {
        "POSTGRES_DB": PRIMARY_DB_NAME,
        "POSTGRES_USER": PRIMARY_DB_USER,
        "POSTGRES_PASSWORD": password,
    }
    _run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "--network",
            network_name,
            "--network-alias",
            network_alias,
            "-p",
            f"{host_port}:5432",
            "-v",
            f"{volume_name}:/var/lib/postgresql/data",
            *_docker_env_args(postgres_env),
            "postgres:16-alpine",
        ]
    )
    _wait_for_postgres(container_name, password, timeout_seconds=90)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the external-HA PostgreSQL rehearsal against the documented manifests."
    )
    parser.add_argument("--image-tag", default="etl-identity-engine:ha-rehearsal")
    parser.add_argument("--service-port", default=18082, type=int)
    parser.add_argument("--writer-port", default=55440, type=int)
    parser.add_argument("--restore-port", default=55441, type=int)
    parser.add_argument("--reader-api-key", default="cluster-ha-reader-secret")
    parser.add_argument("--operator-api-key", default="cluster-ha-operator-secret")
    args = parser.parse_args(argv)

    commands = _validate_manifests()

    workspace = Path(tempfile.mkdtemp(prefix="etl-identity-engine-ha-rehearsal-"))
    primary_runtime_root = workspace / "primary-runtime"
    restored_runtime_root = workspace / "restored-runtime"
    rehearsal_root = workspace / "rehearsal"
    _ensure_runtime_root_permissions(primary_runtime_root)
    _ensure_runtime_root_permissions(restored_runtime_root)
    rehearsal_root.mkdir(parents=True, exist_ok=True)
    _set_world_access(rehearsal_root)
    _prepare_landing_zone(primary_runtime_root)

    passphrase_file = rehearsal_root / "backup-passphrase.txt"
    passphrase_file.write_text("ha-rehearsal-passphrase\n", encoding="utf-8")
    _set_world_access(passphrase_file)

    network_name = f"etl-identity-ha-rehearsal-{int(time.time())}"
    writer_volume = f"{network_name}-writer-data"
    restore_volume = f"{network_name}-restore-data"
    writer_container_name = f"{network_name}-writer-a"
    replacement_writer_container_name = f"{network_name}-writer-b"
    restore_container_name = f"{network_name}-restore"
    service_container_name = f"{network_name}-service"

    writer_state_db = (
        f"postgresql://{PRIMARY_DB_USER}:{PRIMARY_DB_PASSWORD}@{PRIMARY_WRITER_ALIAS}:5432/{PRIMARY_DB_NAME}"
        "?target_session_attrs=read-write"
    )
    writer_state_db_host = (
        f"postgresql://{PRIMARY_DB_USER}:{PRIMARY_DB_PASSWORD}@127.0.0.1:{args.writer_port}/{PRIMARY_DB_NAME}"
        "?target_session_attrs=read-write"
    )
    restore_state_db = (
        f"postgresql://{PRIMARY_DB_USER}:{RESTORE_DB_PASSWORD}@{RESTORE_WRITER_ALIAS}:5432/{PRIMARY_DB_NAME}"
        "?target_session_attrs=read-write"
    )
    restore_state_db_host = (
        f"postgresql://{PRIMARY_DB_USER}:{RESTORE_DB_PASSWORD}@127.0.0.1:{args.restore_port}/{PRIMARY_DB_NAME}"
        "?target_session_attrs=read-write"
    )
    runtime_env = {
        "ETL_IDENTITY_STATE_DB": writer_state_db,
        "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY": "disabled",
        "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY": "disabled",
        "ETL_IDENTITY_SERVICE_READER_API_KEY": args.reader_api_key,
        "ETL_IDENTITY_SERVICE_OPERATOR_API_KEY": args.operator_api_key,
        "ETL_IDENTITY_SERVICE_READER_TENANT_ID": "default",
        "ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID": "default",
    }

    run_id = ""
    replay_run_id = ""
    try:
        print(f"building image {args.image_tag}")
        _run(["docker", "build", "-t", args.image_tag, "."])

        print("creating rehearsal network and data volumes")
        _run(["docker", "network", "create", network_name])
        _run(["docker", "volume", "create", writer_volume])
        _run(["docker", "volume", "create", restore_volume])

        print("starting initial PostgreSQL writer")
        _start_postgres_writer(
            container_name=writer_container_name,
            network_name=network_name,
            network_alias=PRIMARY_WRITER_ALIAS,
            volume_name=writer_volume,
            host_port=args.writer_port,
            password=PRIMARY_DB_PASSWORD,
        )

        print("running HA state-db upgrade job command")
        _run_container_command(
            image_tag=args.image_tag,
            network_name=network_name,
            env_map=runtime_env,
            args=commands["upgrade_args"],
        )

        print("running HA batch job command")
        _run_container_command(
            image_tag=args.image_tag,
            network_name=network_name,
            env_map=runtime_env,
            args=commands["batch_args"],
            mounts=[(primary_runtime_root, "/runtime")],
        )

        primary_store = SQLitePipelineStore(writer_state_db_host)
        run_id = primary_store.latest_completed_run_id() or ""
        if not run_id:
            raise RuntimeError("Expected the HA rehearsal batch to produce a completed run")

        review_cases = primary_store.list_review_cases(run_id=run_id)
        if len(review_cases) != 1:
            raise RuntimeError(
                f"Expected exactly one review case in the HA rehearsal batch, found {len(review_cases)}"
            )

        _run_local_cli(
            [
                "apply-review-decision",
                "--state-db",
                writer_state_db_host,
                "--run-id",
                run_id,
                "--review-id",
                review_cases[0].review_id,
                "--decision",
                "approved",
                "--assigned-to",
                "ha.rehearsal.operator",
                "--notes",
                "Approved during the HA rehearsal",
            ],
            expect_json=True,
        )

        print("starting HA service deployment command")
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

        latest_run_payload = _wait_for_service_endpoint(
            f"http://127.0.0.1:{args.service_port}/api/v1/runs/latest",
            args.reader_api_key,
            timeout_seconds=90,
        )
        if latest_run_payload.get("run_id") != run_id:
            raise RuntimeError("HA service latest-run endpoint returned an unexpected run")

        print("simulating PostgreSQL writer failover")
        _run(["docker", "rm", "-f", writer_container_name], check=False)
        _start_postgres_writer(
            container_name=replacement_writer_container_name,
            network_name=network_name,
            network_alias=PRIMARY_WRITER_ALIAS,
            volume_name=writer_volume,
            host_port=args.writer_port,
            password=PRIMARY_DB_PASSWORD,
        )
        failover_payload = _wait_for_service_endpoint(
            f"http://127.0.0.1:{args.service_port}/readyz",
            args.reader_api_key,
            timeout_seconds=90,
        )
        if failover_payload.get("status") != "ready":
            raise RuntimeError("HA service readiness did not recover after writer failover")

        print("verifying the archived replay bundle against the writer endpoint")
        _run_container_command(
            image_tag=args.image_tag,
            network_name=network_name,
            env_map=runtime_env,
            args=[
                "verify-replay-bundle",
                "--state-db",
                writer_state_db,
                "--run-id",
                run_id,
            ],
            mounts=[(primary_runtime_root, "/runtime")],
        )

        print("creating encrypted HA backup bundle")
        backup_bundle_path = rehearsal_root / "pipeline_state_backup_encrypted.zip"
        replay_bundle_root = f"/runtime/output/data/replay_bundles/{run_id}"
        _run_container_command(
            image_tag=args.image_tag,
            network_name=network_name,
            env_map=runtime_env,
            args=[
                "backup-state-bundle",
                "--state-db",
                writer_state_db,
                "--output",
                "/rehearsal/pipeline_state_backup_encrypted.zip",
                "--include-path",
                replay_bundle_root,
                "--passphrase-file",
                "/rehearsal/backup-passphrase.txt",
            ],
            mounts=[(primary_runtime_root, "/runtime"), (rehearsal_root, "/rehearsal")],
        )
        if not backup_bundle_path.exists():
            raise RuntimeError("Expected the HA rehearsal backup bundle to be created")

        print("starting clean PostgreSQL restore target")
        _start_postgres_writer(
            container_name=restore_container_name,
            network_name=network_name,
            network_alias=RESTORE_WRITER_ALIAS,
            volume_name=restore_volume,
            host_port=args.restore_port,
            password=RESTORE_DB_PASSWORD,
        )

        print("restoring the encrypted bundle into the clean PostgreSQL target")
        _run_container_command(
            image_tag=args.image_tag,
            network_name=network_name,
            env_map=runtime_env,
            args=[
                "restore-state-bundle",
                "--state-db",
                restore_state_db,
                "--bundle",
                "/rehearsal/pipeline_state_backup_encrypted.zip",
                "--attachments-output-dir",
                "/runtime/output/data/replay_bundles",
                "--passphrase-file",
                "/rehearsal/backup-passphrase.txt",
            ],
            mounts=[(restored_runtime_root, "/runtime"), (rehearsal_root, "/rehearsal")],
        )
        restored_replay_bundle_root = restored_runtime_root / "output" / "data" / "replay_bundles" / run_id
        if not restored_replay_bundle_root.exists():
            raise RuntimeError("Expected restore-state-bundle to restore the archived replay bundle")

        restored_review_cases = _run_local_cli(
            [
                "review-case-list",
                "--state-db",
                restore_state_db_host,
                "--run-id",
                run_id,
            ],
            expect_json=True,
        )
        if len(restored_review_cases) != 1 or restored_review_cases[0]["queue_status"] != "approved":
            raise RuntimeError("Expected the restored PostgreSQL state store to preserve the approved review case")

        print("rebuilding report outputs from restored PostgreSQL state")
        _run_container_command(
            image_tag=args.image_tag,
            network_name=network_name,
            env_map=runtime_env,
            args=[
                "report",
                "--state-db",
                restore_state_db,
                "--run-id",
                run_id,
                "--output",
                "/runtime/recovery/run_report.md",
            ],
            mounts=[(restored_runtime_root, "/runtime")],
        )
        if not (restored_runtime_root / "recovery" / "run_report.md").exists():
            raise RuntimeError("Expected the restored PostgreSQL rehearsal to rebuild report outputs")

        print("replaying the restored run from the archived bundle")
        replay_payload = _run_container_command(
            image_tag=args.image_tag,
            network_name=network_name,
            env_map=runtime_env,
            args=[
                "replay-run",
                "--state-db",
                restore_state_db,
                "--run-id",
                run_id,
                "--base-dir",
                "/runtime/replayed-run",
                "--refresh-mode",
                "incremental",
            ],
            mounts=[(restored_runtime_root, "/runtime")],
        )
        replay_result = json.loads(replay_payload.stdout)
        replay_run_id = str(replay_result["result_run_id"])
        if replay_result.get("action") != "replayed" or not replay_run_id:
            raise RuntimeError("Expected replay-run to create a recovered PostgreSQL run")

        restored_store = SQLitePipelineStore(restore_state_db_host)
        replay_bundle = restored_store.load_run_bundle(replay_run_id)
        if replay_bundle.candidate_pairs[0]["decision"] != "auto_merge":
            raise RuntimeError("Expected the approved review to force an auto-merge during restored replay")
        if "review_case_approved_override" not in replay_bundle.candidate_pairs[0]["reason_trace"]:
            raise RuntimeError("Expected the replayed run to record the approved-review override")
        if len(replay_bundle.golden_rows) != 1:
            raise RuntimeError("Expected the replayed HA rehearsal run to produce one merged golden record")
        if replay_bundle.review_rows[0]["queue_status"] != "approved":
            raise RuntimeError("Expected the replayed HA rehearsal run to preserve approved review state")

        print(
            json.dumps(
                {
                    "status": "ok",
                    "run_id": run_id,
                    "replay_run_id": replay_run_id,
                    "validated_steps": [
                        "ha_manifests_validated",
                        "schema_upgrade_against_writer_endpoint",
                        "batch_run_against_writer_endpoint",
                        "service_reconnected_after_writer_failover",
                        "encrypted_backup_bundle_created",
                        "backup_restored_to_clean_postgresql_target",
                        "report_rebuilt_from_restored_postgresql_state",
                        "replay_recovered_run_from_restored_postgresql_state",
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stdout)
        sys.stderr.write(exc.stderr)
        raise
    finally:
        for container_name in (
            service_container_name,
            writer_container_name,
            replacement_writer_container_name,
            restore_container_name,
        ):
            _run(["docker", "rm", "-f", container_name], check=False)
        for volume_name in (writer_volume, restore_volume):
            _run(["docker", "volume", "rm", "-f", volume_name], check=False)
        _run(["docker", "network", "rm", network_name], check=False)
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
