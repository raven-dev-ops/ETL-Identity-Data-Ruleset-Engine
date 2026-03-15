from __future__ import annotations

import csv
from pathlib import Path
import shutil

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


def _write_export_jobs_config(config_dir: Path, *, output_root: Path) -> Path:
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "export_jobs.yml"
    config_path.write_text(
        f"""
export_jobs:
  - name: service_api_identity_snapshot
    consumer: warehouse
    description: Materialize a service API test export snapshot.
    output_root: {output_root.as_posix()}
    contract_name: golden_crosswalk_snapshot
    contract_version: v1
    format: csv_snapshot
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config_path


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


def _create_persisted_manifest_run(
    tmp_path: Path,
    *,
    db_path: Path | None = None,
    batch_id: str = "service-api-001",
    run_dir_name: str = "run",
    manifest_dir_name: str = "manifest-run",
) -> tuple[Path, str, SQLitePipelineStore]:
    db_path = db_path or (tmp_path / "state" / "pipeline_state.sqlite")
    base_dir = tmp_path / run_dir_name
    manifest_dir = tmp_path / manifest_dir_name
    landing_dir = manifest_dir / "landing"
    manifest_path = _write_manifest(
        manifest_dir / "manifest.yml",
        batch_id=batch_id,
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


def _create_persisted_synthetic_run(
    tmp_path: Path,
    *,
    db_path: Path,
    seed: int = 42,
    profile: str = "small",
    base_dir_name: str = "synthetic-run",
) -> tuple[str, SQLitePipelineStore]:
    base_dir = tmp_path / base_dir_name
    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                profile,
                "--seed",
                str(seed),
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )
    store = SQLitePipelineStore(db_path)
    run_id = store.latest_completed_run_id()
    assert run_id is not None
    return run_id, store


def _create_public_safety_manifest_run(
    tmp_path: Path,
    *,
    db_path: Path | None = None,
    run_dir_name: str = "public-safety-run",
) -> tuple[Path, str, SQLitePipelineStore]:
    db_path = db_path or (tmp_path / "state" / "pipeline_state.sqlite")
    base_dir = tmp_path / run_dir_name
    fixture_source = Path(__file__).resolve().parents[1] / "fixtures" / "public_safety_onboarding"
    fixture_root = tmp_path / "public-safety-onboarding"
    shutil.copytree(fixture_source, fixture_root)
    manifest_path = fixture_root / "example_manifest.yml"

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
        scope_claim="scope",
        reader_roles=("etl-reader",),
        operator_roles=("etl-operator",),
        reader_scopes=(
            "service:health",
            "service:metrics",
            "runs:read",
            "golden:read",
            "crosswalk:read",
            "public_safety:read",
            "review_cases:read",
        ),
        operator_scopes=(
            "service:health",
            "service:metrics",
            "runs:read",
            "golden:read",
            "crosswalk:read",
            "public_safety:read",
            "review_cases:read",
            "review_cases:write",
            "runs:replay",
            "runs:publish",
            "exports:run",
        ),
        subject_claim="preferred_username",
    )


def _jwt_headers(*roles: str, username: str = "analyst.one", scopes: tuple[str, ...] | None = None) -> dict[str, str]:
    token = jwt.encode(
        {
            "iss": "https://idp.example.test",
            "aud": "etl-identity-api",
            "sub": f"subject-{username}",
            "preferred_username": username,
            "realm_access": {"roles": list(roles)},
            "scope": " ".join(scopes or ()),
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


def test_service_api_exposes_paginated_run_list_with_filters_and_search(tmp_path: Path) -> None:
    db_path, first_run_id, _store = _create_persisted_manifest_run(
        tmp_path,
        batch_id="service-api-001",
        run_dir_name="manifest-run-1",
        manifest_dir_name="manifest-source-1",
    )
    _, second_run_id, _store = _create_persisted_manifest_run(
        tmp_path,
        db_path=db_path,
        batch_id="service-api-002",
        run_dir_name="manifest-run-2",
        manifest_dir_name="manifest-source-2",
    )
    synthetic_run_id, _store = _create_persisted_synthetic_run(
        tmp_path,
        db_path=db_path,
        seed=7,
        base_dir_name="synthetic-run-7",
    )

    client = TestClient(create_service_app(db_path, service_auth=_service_auth()))
    reader_headers = _auth_headers("reader-secret")

    first_page_response = client.get(
        "/api/v1/runs",
        headers=reader_headers,
        params={"page_size": 2, "sort": "started_at_asc"},
    )
    assert first_page_response.status_code == 200
    first_page = first_page_response.json()
    assert first_page["page"] == {
        "page_size": 2,
        "total_count": 3,
        "next_page_token": "2",
        "sort": "started_at_asc",
    }
    assert len(first_page["items"]) == 2
    assert {item["run_id"] for item in first_page["items"]} == {first_run_id, second_run_id}

    second_page_response = client.get(
        "/api/v1/runs",
        headers=reader_headers,
        params={
            "page_size": 2,
            "sort": "started_at_asc",
            "page_token": first_page["page"]["next_page_token"],
        },
    )
    assert second_page_response.status_code == 200
    second_page = second_page_response.json()
    assert second_page["page"]["next_page_token"] is None
    assert [item["run_id"] for item in second_page["items"]] == [synthetic_run_id]

    manifest_only_response = client.get(
        "/api/v1/runs",
        headers=reader_headers,
        params={"page_size": 10, "input_mode": "manifest", "sort": "started_at_asc"},
    )
    assert manifest_only_response.status_code == 200
    assert [item["run_id"] for item in manifest_only_response.json()["items"]] == [
        first_run_id,
        second_run_id,
    ]

    batch_filter_response = client.get(
        "/api/v1/runs",
        headers=reader_headers,
        params={"page_size": 10, "batch_id": "service-api-002"},
    )
    assert batch_filter_response.status_code == 200
    assert [item["run_id"] for item in batch_filter_response.json()["items"]] == [second_run_id]

    query_filter_response = client.get(
        "/api/v1/runs",
        headers=reader_headers,
        params={"page_size": 10, "query": "synthetic-run-7"},
    )
    assert query_filter_response.status_code == 200
    assert [item["run_id"] for item in query_filter_response.json()["items"]] == [synthetic_run_id]

    invalid_page_token_response = client.get(
        "/api/v1/runs",
        headers=reader_headers,
        params={"page_token": "bad-token"},
    )
    assert invalid_page_token_response.status_code == 422


def test_service_api_exposes_paginated_golden_and_review_case_lists(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    run_id, store = _create_persisted_synthetic_run(
        tmp_path,
        db_path=db_path,
        seed=42,
        base_dir_name="synthetic-run-42",
    )
    bundle = store.load_run_bundle(run_id)
    assert len(bundle.golden_rows) > 2
    assert len(bundle.review_rows) > 2

    client = TestClient(create_service_app(db_path, service_auth=_service_auth()))
    reader_headers = _auth_headers("reader-secret")

    golden_page_response = client.get(
        f"/api/v1/runs/{run_id}/golden-records",
        headers=reader_headers,
        params={"page_size": 2, "sort": "golden_id_asc"},
    )
    assert golden_page_response.status_code == 200
    golden_page = golden_page_response.json()
    assert golden_page["page"]["total_count"] == len(bundle.golden_rows)
    assert golden_page["page"]["next_page_token"] == "2"
    assert [item["golden_id"] for item in golden_page["items"]] == [
        bundle.golden_rows[0]["golden_id"],
        bundle.golden_rows[1]["golden_id"],
    ]

    golden_second_page_response = client.get(
        f"/api/v1/runs/{run_id}/golden-records",
        headers=reader_headers,
        params={"page_size": 2, "sort": "golden_id_asc", "page_token": "2"},
    )
    assert golden_second_page_response.status_code == 200
    assert [item["golden_id"] for item in golden_second_page_response.json()["items"]] == [
        bundle.golden_rows[2]["golden_id"],
        bundle.golden_rows[3]["golden_id"],
    ]

    first_golden = bundle.golden_rows[0]
    golden_filter_response = client.get(
        f"/api/v1/runs/{run_id}/golden-records",
        headers=reader_headers,
        params={"page_size": 10, "person_entity_id": first_golden["person_entity_id"]},
    )
    assert golden_filter_response.status_code == 200
    assert [item["golden_id"] for item in golden_filter_response.json()["items"]] == [
        first_golden["golden_id"]
    ]

    golden_query_response = client.get(
        f"/api/v1/runs/{run_id}/golden-records",
        headers=reader_headers,
        params={"page_size": 10, "query": first_golden["last_name"]},
    )
    assert golden_query_response.status_code == 200
    assert any(
        item["golden_id"] == first_golden["golden_id"]
        for item in golden_query_response.json()["items"]
    )

    review_page_response = client.get(
        f"/api/v1/runs/{run_id}/review-cases/page",
        headers=reader_headers,
        params={"page_size": 2, "sort": "score_desc"},
    )
    assert review_page_response.status_code == 200
    review_page = review_page_response.json()
    assert review_page["page"]["total_count"] == len(bundle.review_rows)
    assert review_page["page"]["next_page_token"] == "2"
    assert len(review_page["items"]) == 2
    assert review_page["items"][0]["score"] >= review_page["items"][1]["score"]

    review_second_page_response = client.get(
        f"/api/v1/runs/{run_id}/review-cases/page",
        headers=reader_headers,
        params={"page_size": 2, "sort": "score_desc", "page_token": "2"},
    )
    assert review_second_page_response.status_code == 200
    assert len(review_second_page_response.json()["items"]) == len(bundle.review_rows) - 2

    first_review = bundle.review_rows[0]
    review_query_response = client.get(
        f"/api/v1/runs/{run_id}/review-cases/page",
        headers=reader_headers,
        params={"page_size": 10, "query": first_review["review_id"]},
    )
    assert review_query_response.status_code == 200
    assert [item["review_id"] for item in review_query_response.json()["items"]] == [
        first_review["review_id"]
    ]

    pending_only_response = client.get(
        f"/api/v1/runs/{run_id}/review-cases/page",
        headers=reader_headers,
        params={"page_size": 10, "status": "pending"},
    )
    assert pending_only_response.status_code == 200
    assert pending_only_response.json()["page"]["total_count"] == len(bundle.review_rows)

    missing_run_response = client.get(
        "/api/v1/runs/RUN-DOES-NOT-EXIST/golden-records",
        headers=reader_headers,
        params={"page_size": 10},
    )
    assert missing_run_response.status_code == 404


def test_service_api_exposes_public_safety_activity_read_models(tmp_path: Path) -> None:
    db_path, run_id, store = _create_public_safety_manifest_run(tmp_path)
    bundle = store.load_run_bundle(run_id)
    assert bundle.public_safety_golden_activity_rows
    assert bundle.public_safety_incident_identity_rows

    client = TestClient(create_service_app(db_path, service_auth=_service_auth()))
    reader_headers = _auth_headers("reader-secret")

    activity_page_response = client.get(
        f"/api/v1/runs/{run_id}/public-safety/golden-activity",
        headers=reader_headers,
        params={"page_size": 2, "sort": "total_incident_desc"},
    )
    assert activity_page_response.status_code == 200
    activity_page = activity_page_response.json()
    assert activity_page["page"]["total_count"] == len(bundle.public_safety_golden_activity_rows)
    assert activity_page["page"]["sort"] == "total_incident_desc"
    assert len(activity_page["items"]) == 2
    assert int(activity_page["items"][0]["total_incident_count"]) >= int(
        activity_page["items"][1]["total_incident_count"]
    )

    first_activity = bundle.public_safety_golden_activity_rows[0]
    activity_detail_response = client.get(
        f"/api/v1/runs/{run_id}/public-safety/golden-activity/{first_activity['golden_id']}",
        headers=reader_headers,
    )
    assert activity_detail_response.status_code == 200
    assert activity_detail_response.json()["golden_id"] == first_activity["golden_id"]

    activity_query_response = client.get(
        f"/api/v1/runs/{run_id}/public-safety/golden-activity",
        headers=reader_headers,
        params={"page_size": 10, "query": first_activity["golden_last_name"]},
    )
    assert activity_query_response.status_code == 200
    assert any(
        item["golden_id"] == first_activity["golden_id"]
        for item in activity_query_response.json()["items"]
    )

    first_incident = bundle.public_safety_incident_identity_rows[0]
    incident_page_response = client.get(
        f"/api/v1/runs/{run_id}/public-safety/incidents",
        headers=reader_headers,
        params={
            "page_size": 10,
            "golden_id": first_incident["golden_id"],
            "incident_source_system": first_incident["incident_source_system"],
            "sort": "occurred_at_desc",
        },
    )
    assert incident_page_response.status_code == 200
    incident_page = incident_page_response.json()
    assert incident_page["page"]["total_count"] >= 1
    assert all(
        item["golden_id"] == first_incident["golden_id"]
        and item["incident_source_system"] == first_incident["incident_source_system"]
        for item in incident_page["items"]
    )

    incident_query_response = client.get(
        f"/api/v1/runs/{run_id}/public-safety/incidents",
        headers=reader_headers,
        params={"page_size": 10, "query": first_incident["incident_id"]},
    )
    assert incident_query_response.status_code == 200
    assert any(
        item["incident_id"] == first_incident["incident_id"]
        for item in incident_query_response.json()["items"]
    )

    missing_run_response = client.get(
        "/api/v1/runs/RUN-DOES-NOT-EXIST/public-safety/incidents",
        headers=reader_headers,
        params={"page_size": 10},
    )
    assert missing_run_response.status_code == 404

def test_service_api_requires_authentication_and_operator_role_for_mutations(tmp_path: Path) -> None:
    db_path, run_id, store = _create_persisted_manifest_run(tmp_path)
    manifest_path = tmp_path / "manifest-run" / "manifest.yml"
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
    manifest_path = tmp_path / "manifest-run" / "manifest.yml"
    client = TestClient(create_service_app(db_path, service_auth=_jwt_service_auth()))
    reader_headers = _jwt_headers(
        "etl-reader",
        username="reader.user",
        scopes=("service:health", "runs:read", "public_safety:read", "review_cases:read"),
    )
    operator_review_headers = _jwt_headers(
        "etl-operator",
        username="review.operator",
        scopes=(
            "service:health",
            "runs:read",
            "public_safety:read",
            "review_cases:read",
            "review_cases:write",
        ),
    )
    operator_replay_headers = _jwt_headers(
        "etl-operator",
        username="replay.operator",
        scopes=("service:health", "runs:read", "public_safety:read", "runs:replay"),
    )

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
        headers=operator_review_headers,
        json={"decision": "approved", "notes": "approved via jwt"},
    )
    assert operator_response.status_code == 200
    assert operator_response.json()["case"]["queue_status"] == "approved"

    updated_manifest = manifest_path.read_text(encoding="utf-8").replace(
        "batch_id: service-api-001",
        "batch_id: service-api-003",
    )
    manifest_path.write_text(updated_manifest, encoding="utf-8")

    missing_scope_response = client.post(
        f"/api/v1/runs/{run_id}/replay",
        headers=operator_review_headers,
        json={"base_dir": str(tmp_path / "jwt-missing-scope"), "refresh_mode": "incremental"},
    )
    assert missing_scope_response.status_code == 403
    assert "runs:replay" in missing_scope_response.json()["detail"]

    replay_response = client.post(
        f"/api/v1/runs/{run_id}/replay",
        headers=operator_replay_headers,
        json={"base_dir": str(tmp_path / "jwt-replay"), "refresh_mode": "incremental"},
    )
    assert replay_response.status_code == 200
    assert replay_response.json()["action"] == "replayed"

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
        event.action == "apply_review_decision"
        and event.actor_id == "review.operator"
        and event.details["actor_role"] == "operator"
        and event.details["actor_subject"] == "review.operator"
        and str(event.details["operator_notes"]).startswith("[REDACTED free_text len=")
        and event.details["required_scopes"] == ["review_cases:write"]
        and event.details["auth_mode"] == "jwt"
        for event in audit_events
    )
    assert any(
        event.action == "replay_run"
        and event.actor_id == "replay.operator"
        and event.details["required_scopes"] == ["runs:replay"]
        and "runs:replay" in event.details["granted_scopes"]
        for event in audit_events
    )


def test_service_api_exposes_publish_and_export_job_triggers(tmp_path: Path) -> None:
    db_path, run_id, store = _create_persisted_manifest_run(tmp_path)
    publish_root = tmp_path / "service-publish"
    export_root = tmp_path / "service-exports"
    config_dir = tmp_path / "service-config"
    _write_export_jobs_config(config_dir, output_root=export_root)

    client = TestClient(
        create_service_app(
            db_path,
            service_auth=_service_auth(),
            config_dir=config_dir,
        )
    )
    reader_headers = _auth_headers("reader-secret")
    operator_headers = _auth_headers("operator-secret")

    reader_publish_response = client.post(
        f"/api/v1/runs/{run_id}/publish",
        headers=reader_headers,
        json={"output_dir": str(publish_root)},
    )
    assert reader_publish_response.status_code == 403

    operator_publish_response = client.post(
        f"/api/v1/runs/{run_id}/publish",
        headers=operator_headers,
        json={"output_dir": str(publish_root)},
    )
    assert operator_publish_response.status_code == 200
    assert operator_publish_response.json()["action"] == "published"
    assert Path(operator_publish_response.json()["snapshot_dir"]).exists()
    assert Path(operator_publish_response.json()["current_pointer_path"]).exists()

    reused_publish_response = client.post(
        f"/api/v1/runs/{run_id}/publish",
        headers=operator_headers,
        json={"output_dir": str(publish_root)},
    )
    assert reused_publish_response.status_code == 200
    assert reused_publish_response.json()["action"] == "reused_snapshot"

    reader_export_response = client.post(
        f"/api/v1/runs/{run_id}/exports/service_api_identity_snapshot",
        headers=reader_headers,
    )
    assert reader_export_response.status_code == 403

    operator_export_response = client.post(
        f"/api/v1/runs/{run_id}/exports/service_api_identity_snapshot",
        headers=operator_headers,
    )
    assert operator_export_response.status_code == 200
    assert operator_export_response.json()["action"] == "exported"
    assert operator_export_response.json()["job"]["name"] == "service_api_identity_snapshot"
    assert Path(operator_export_response.json()["export_run"]["snapshot_dir"]).exists()
    assert Path(operator_export_response.json()["export_run"]["current_pointer_path"]).exists()

    reused_export_response = client.post(
        f"/api/v1/runs/{run_id}/exports/service_api_identity_snapshot",
        headers=operator_headers,
    )
    assert reused_export_response.status_code == 200
    assert reused_export_response.json()["action"] == "reused_completed_export"

    audit_events = store.list_audit_events(run_id=run_id, limit=20)
    assert any(
        event.action == "publish_run"
        and event.actor_type == "service_api"
        and event.details["required_scopes"] == ["runs:publish"]
        and event.details["action"] in {"published", "reused_snapshot"}
        for event in audit_events
    )
    assert any(
        event.action == "export_job_run"
        and event.actor_type == "service_api"
        and event.details["required_scopes"] == ["exports:run"]
        and event.details["action"] in {"exported", "reused_completed_export"}
        for event in audit_events
    )


def test_service_api_enforces_publish_and_export_scopes_for_jwt_tokens(tmp_path: Path) -> None:
    db_path, run_id, store = _create_persisted_manifest_run(tmp_path)
    publish_root = tmp_path / "jwt-publish"
    export_root = tmp_path / "jwt-exports"
    config_dir = tmp_path / "service-config"
    _write_export_jobs_config(config_dir, output_root=export_root)

    client = TestClient(
        create_service_app(
            db_path,
            service_auth=_jwt_service_auth(),
            config_dir=config_dir,
        )
    )

    missing_publish_scope_headers = _jwt_headers(
        "etl-operator",
        username="missing.publish.scope",
        scopes=("runs:read",),
    )
    publish_headers = _jwt_headers(
        "etl-operator",
        username="publish.operator",
        scopes=("runs:publish",),
    )
    missing_export_scope_headers = _jwt_headers(
        "etl-operator",
        username="missing.export.scope",
        scopes=("runs:publish",),
    )
    export_headers = _jwt_headers(
        "etl-operator",
        username="export.operator",
        scopes=("public_safety:read", "exports:run"),
    )

    missing_publish_scope_response = client.post(
        f"/api/v1/runs/{run_id}/publish",
        headers=missing_publish_scope_headers,
        json={"output_dir": str(publish_root)},
    )
    assert missing_publish_scope_response.status_code == 403
    assert "runs:publish" in missing_publish_scope_response.json()["detail"]

    publish_response = client.post(
        f"/api/v1/runs/{run_id}/publish",
        headers=publish_headers,
        json={"output_dir": str(publish_root)},
    )
    assert publish_response.status_code == 200
    assert publish_response.json()["action"] == "published"

    missing_export_scope_response = client.post(
        f"/api/v1/runs/{run_id}/exports/service_api_identity_snapshot",
        headers=missing_export_scope_headers,
    )
    assert missing_export_scope_response.status_code == 403
    assert "exports:run" in missing_export_scope_response.json()["detail"]

    export_response = client.post(
        f"/api/v1/runs/{run_id}/exports/service_api_identity_snapshot",
        headers=export_headers,
    )
    assert export_response.status_code == 200
    assert export_response.json()["action"] == "exported"

    audit_events = store.list_audit_events(run_id=run_id, limit=20)
    assert any(
        event.action == "publish_run"
        and event.actor_id == "publish.operator"
        and event.details["required_scopes"] == ["runs:publish"]
        and "runs:publish" in event.details["granted_scopes"]
        and event.details["auth_mode"] == "jwt"
        for event in audit_events
    )
    assert any(
        event.action == "export_job_run"
        and event.actor_id == "export.operator"
        and event.details["required_scopes"] == ["exports:run"]
        and "exports:run" in event.details["granted_scopes"]
        and event.details["auth_mode"] == "jwt"
        for event in audit_events
    )


def test_service_api_enforces_public_safety_read_scope_for_jwt_tokens(tmp_path: Path) -> None:
    db_path, run_id, _store = _create_public_safety_manifest_run(tmp_path)
    client = TestClient(create_service_app(db_path, service_auth=_jwt_service_auth()))

    missing_scope_headers = _jwt_headers(
        "etl-reader",
        username="missing.public.safety.scope",
        scopes=("runs:read",),
    )
    allowed_headers = _jwt_headers(
        "etl-reader",
        username="public.safety.reader",
        scopes=("runs:read", "public_safety:read"),
    )

    forbidden_response = client.get(
        f"/api/v1/runs/{run_id}/public-safety/golden-activity",
        headers=missing_scope_headers,
        params={"page_size": 10},
    )
    assert forbidden_response.status_code == 403
    assert "public_safety:read" in forbidden_response.json()["detail"]

    allowed_response = client.get(
        f"/api/v1/runs/{run_id}/public-safety/golden-activity",
        headers=allowed_headers,
        params={"page_size": 10},
    )
    assert allowed_response.status_code == 200
