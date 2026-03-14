from __future__ import annotations

import csv
from pathlib import Path

import jwt
import pytest
from fastapi.testclient import TestClient

from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.runtime_config import ServiceAuthConfig
from etl_identity_engine.service_api import create_service_app
from etl_identity_engine.storage.sqlite_store import SQLitePipelineStore


JWT_TEST_SECRET = "shared-signing-secret-material-32b"


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


def _write_manifest(path: Path, *, batch_id: str, source_a_path: str, source_b_path: str) -> Path:
    required_columns = "\n".join(f"        - {column}" for column in PERSON_HEADERS)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""
manifest_version: "1.0"
entity_type: person
batch_id: {batch_id}
landing_zone:
  kind: local_filesystem
  base_path: ./landing
sources:
  - source_id: source_a
    path: {source_a_path}
    format: csv
    schema_version: person-v1
    required_columns:
{required_columns}
  - source_id: source_b
    path: {source_b_path}
    format: parquet
    schema_version: person-v1
    required_columns:
{required_columns}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _create_persisted_manifest_run(tmp_path: Path) -> tuple[Path, str, SQLitePipelineStore]:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="service-api-001",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )
    source_a_rows = [
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
    ]
    source_b_rows = [
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
    ]
    _write_csv_rows(landing_dir / "agency_a.csv", source_a_rows)
    _write_parquet_rows(landing_dir / "agency_b.parquet", source_b_rows)

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--manifest",
                str(manifest_path),
                "--state-db",
                str(db_path),
                "--refresh-mode",
                "full",
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    run_id = store.latest_completed_run_id()
    assert run_id is not None
    return db_path, run_id, store


def _service_auth() -> ServiceAuthConfig:
    return ServiceAuthConfig(
        header_name="X-API-Key",
        reader_api_key="reader-secret",
        operator_api_key="operator-secret",
    )


def _auth_headers(api_key: str) -> dict[str, str]:
    return {"X-API-Key": api_key}


def _jwt_service_auth() -> ServiceAuthConfig:
    return ServiceAuthConfig(
        header_name="Authorization",
        mode="jwt",
        issuer="https://idp.example.test",
        audience="etl-identity-api",
        algorithms=("HS256",),
        jwt_secret=JWT_TEST_SECRET,
        role_claim="realm_access.roles",
        reader_roles=("etl-reader",),
        operator_roles=("etl-operator",),
        subject_claim="preferred_username",
    )


def _jwt_headers(*roles: str, username: str = "analyst.one") -> dict[str, str]:
    token = jwt.encode(
        {
            "iss": "https://idp.example.test",
            "aud": "etl-identity-api",
            "sub": f"subject-{username}",
            "preferred_username": username,
            "realm_access": {"roles": list(roles)},
        },
        JWT_TEST_SECRET,
        algorithm="HS256",
    )
    return {"Authorization": f"Bearer {token}"}


def test_service_api_exposes_run_golden_crosswalk_and_review_state(tmp_path: Path) -> None:
    db_path, run_id, store = _create_persisted_manifest_run(tmp_path)
    bundle = store.load_run_bundle(run_id)
    crosswalk_row = bundle.crosswalk_rows[0]
    review_row = bundle.review_rows[0]

    client = TestClient(create_service_app(db_path, service_auth=_service_auth()))
    reader_headers = _auth_headers("reader-secret")

    health_response = client.get("/healthz", headers=reader_headers)
    assert health_response.status_code == 200
    assert health_response.json()["status"] == "ok"
    assert health_response.json()["service_started_at_utc"]

    readiness_response = client.get("/readyz", headers=reader_headers)
    assert readiness_response.status_code == 200
    assert readiness_response.json()["status"] == "ready"
    assert readiness_response.json()["latest_completed_run_id"] == run_id

    metrics_response = client.get("/api/v1/metrics", headers=reader_headers)
    assert metrics_response.status_code == 200
    assert metrics_response.json()["runs"]["completed"] == 1
    assert metrics_response.json()["runs"]["running"] == 0
    assert metrics_response.json()["review_cases"]["pending"] == 1
    assert metrics_response.json()["audit_event_count"] == 0

    latest_run_response = client.get("/api/v1/runs/latest", headers=reader_headers)
    assert latest_run_response.status_code == 200
    assert latest_run_response.json()["run_id"] == run_id
    assert latest_run_response.json()["status"] == "completed"

    run_response = client.get(f"/api/v1/runs/{run_id}", headers=reader_headers)
    assert run_response.status_code == 200
    assert run_response.json()["summary"]["run_context"]["input_mode"] == "manifest"

    crosswalk_response = client.get(
        f"/api/v1/runs/{run_id}/crosswalk/source-records/{crosswalk_row['source_record_id']}",
        headers=reader_headers,
    )
    assert crosswalk_response.status_code == 200
    assert crosswalk_response.json() == crosswalk_row

    golden_response = client.get(
        f"/api/v1/runs/{run_id}/golden-records/{crosswalk_row['golden_id']}",
        headers=reader_headers,
    )
    assert golden_response.status_code == 200
    assert golden_response.json()["golden_id"] == crosswalk_row["golden_id"]
    assert golden_response.json()["cluster_id"] == crosswalk_row["cluster_id"]

    review_list_response = client.get(f"/api/v1/runs/{run_id}/review-cases", headers=reader_headers)
    assert review_list_response.status_code == 200
    assert review_list_response.json()[0]["review_id"] == review_row["review_id"]
    assert review_list_response.json()[0]["queue_status"] == "pending"

    review_detail_response = client.get(
        f"/api/v1/runs/{run_id}/review-cases/{review_row['review_id']}",
        headers=reader_headers,
    )
    assert review_detail_response.status_code == 200
    assert review_detail_response.json()["left_id"] == review_row["left_id"]
    assert review_detail_response.json()["right_id"] == review_row["right_id"]


def test_service_api_validates_request_inputs_and_returns_not_found_for_missing_rows(
    tmp_path: Path,
) -> None:
    db_path, run_id, _store = _create_persisted_manifest_run(tmp_path)
    client = TestClient(create_service_app(db_path, service_auth=_service_auth()))
    reader_headers = _auth_headers("reader-secret")

    invalid_status_response = client.get(
        f"/api/v1/runs/{run_id}/review-cases",
        params={"status": "not-a-valid-status"},
        headers=reader_headers,
    )
    assert invalid_status_response.status_code == 422

    invalid_run_id_response = client.get("/api/v1/runs/not-a-run-id", headers=reader_headers)
    assert invalid_run_id_response.status_code == 422

    missing_golden_response = client.get(
        f"/api/v1/runs/{run_id}/golden-records/G-99999",
        headers=reader_headers,
    )
    assert missing_golden_response.status_code == 404

    missing_crosswalk_response = client.get(
        f"/api/v1/runs/{run_id}/crosswalk/source-records/UNKNOWN-RECORD",
        headers=reader_headers,
    )
    assert missing_crosswalk_response.status_code == 404

    with pytest.raises(FileNotFoundError, match="Persisted state database not found"):
        create_service_app(tmp_path / "missing.sqlite", service_auth=_service_auth())


def test_service_api_requires_authentication_and_operator_role_for_mutations(tmp_path: Path) -> None:
    db_path, run_id, store = _create_persisted_manifest_run(tmp_path)
    manifest_path = tmp_path / "manifest.yml"
    review_row = store.list_review_cases(run_id=run_id)[0]
    client = TestClient(create_service_app(db_path, service_auth=_service_auth()))
    reader_headers = _auth_headers("reader-secret")
    operator_headers = _auth_headers("operator-secret")

    missing_auth_response = client.get("/healthz")
    assert missing_auth_response.status_code == 401

    invalid_auth_response = client.get("/healthz", headers=_auth_headers("wrong-secret"))
    assert invalid_auth_response.status_code == 401

    reader_forbidden_response = client.post(
        f"/api/v1/runs/{run_id}/review-cases/{review_row.review_id}/decision",
        headers=reader_headers,
        json={"decision": "approved", "notes": "reader should not mutate"},
    )
    assert reader_forbidden_response.status_code == 403

    operator_review_response = client.post(
        f"/api/v1/runs/{run_id}/review-cases/{review_row.review_id}/decision",
        headers=operator_headers,
        json={
            "decision": "approved",
            "assigned_to": "analyst.api",
            "notes": "Approved via authenticated API",
        },
    )
    assert operator_review_response.status_code == 200
    assert operator_review_response.json()["action"] == "updated"
    assert operator_review_response.json()["case"]["queue_status"] == "approved"

    updated_manifest = manifest_path.read_text(encoding="utf-8").replace(
        "batch_id: service-api-001",
        "batch_id: service-api-002",
    )
    manifest_path.write_text(updated_manifest, encoding="utf-8")

    reader_replay_response = client.post(
        f"/api/v1/runs/{run_id}/replay",
        headers=reader_headers,
        json={"base_dir": str(tmp_path / "reader-replay"), "refresh_mode": "incremental"},
    )
    assert reader_replay_response.status_code == 403

    operator_replay_response = client.post(
        f"/api/v1/runs/{run_id}/replay",
        headers=operator_headers,
        json={"base_dir": str(tmp_path / "operator-replay"), "refresh_mode": "incremental"},
    )
    assert operator_replay_response.status_code == 200
    assert operator_replay_response.json()["action"] == "replayed"
    assert operator_replay_response.json()["requested_run_id"] == run_id
    assert operator_replay_response.json()["result_run_id"] != run_id
    assert operator_replay_response.json()["refresh_mode"] == "incremental"

    metrics_response = client.get("/api/v1/metrics", headers=reader_headers)
    assert metrics_response.status_code == 200
    assert metrics_response.json()["audit_event_count"] >= 2
    assert metrics_response.json()["runs"]["completed"] >= 2

    audit_events = store.list_audit_events(limit=10)
    assert {event.action for event in audit_events} >= {"apply_review_decision", "replay_run"}
    assert all(event.actor_type == "service_api" for event in audit_events[:2])


def test_service_api_supports_jwt_bearer_auth_with_external_identity_claims(tmp_path: Path) -> None:
    db_path, run_id, store = _create_persisted_manifest_run(tmp_path)
    review_row = store.list_review_cases(run_id=run_id)[0]
    client = TestClient(create_service_app(db_path, service_auth=_jwt_service_auth()))
    reader_headers = _jwt_headers("etl-reader", username="reader.user")
    operator_headers = _jwt_headers("etl-operator", username="operator.user")

    health_response = client.get("/healthz", headers=reader_headers)
    assert health_response.status_code == 200

    run_response = client.get(f"/api/v1/runs/{run_id}", headers=reader_headers)
    assert run_response.status_code == 200
    assert run_response.json()["run_id"] == run_id

    forbidden_reader_response = client.post(
        f"/api/v1/runs/{run_id}/review-cases/{review_row.review_id}/decision",
        headers=reader_headers,
        json={"decision": "approved", "notes": "reader token cannot mutate"},
    )
    assert forbidden_reader_response.status_code == 403

    operator_response = client.post(
        f"/api/v1/runs/{run_id}/review-cases/{review_row.review_id}/decision",
        headers=operator_headers,
        json={"decision": "approved", "notes": "approved via jwt"},
    )
    assert operator_response.status_code == 200
    assert operator_response.json()["case"]["queue_status"] == "approved"

    invalid_token_response = client.get(
        "/healthz",
        headers={"Authorization": "Bearer not-a-valid-token"},
    )
    assert invalid_token_response.status_code == 401

    unmapped_role_response = client.get(
        "/healthz",
        headers=_jwt_headers("unmapped-role", username="unknown.user"),
    )
    assert unmapped_role_response.status_code == 403

    audit_events = store.list_audit_events(limit=10)
    assert any(
        event.action == "apply_review_decision" and event.actor_id == "operator.user"
        for event in audit_events
    )
