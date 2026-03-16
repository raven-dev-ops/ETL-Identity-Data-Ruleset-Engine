from __future__ import annotations

from contextlib import closing
import csv
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

import etl_identity_engine.cli as cli_module
from etl_identity_engine.cli import main
from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.storage.sqlite_store import (
    PIPELINE_STATE_TABLES,
    SQLitePipelineStore,
    bootstrap_sqlite_store,
)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_pipeline_runs(db_path: Path) -> list[sqlite3.Row]:
    with closing(sqlite3.connect(db_path)) as connection:
        connection.row_factory = sqlite3.Row
        return connection.execute(
            """
            SELECT run_id, run_key, attempt_number, batch_id, status, started_at_utc, finished_at_utc, failure_detail
            FROM pipeline_runs
            ORDER BY attempt_number ASC, run_id ASC
            """
        ).fetchall()


def _write_config_copy(target_dir: Path) -> None:
    source_dir = Path(__file__).resolve().parents[1] / "config"
    target_dir.mkdir(parents=True, exist_ok=True)
    for source_path in source_dir.glob("*.yml"):
        (target_dir / source_path.name).write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")


def _write_parquet_rows(path: Path, rows: list[dict[str, str]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _copy_public_safety_onboarding_fixture(target_dir: Path) -> Path:
    source_dir = Path(__file__).resolve().parents[1] / "fixtures" / "public_safety_onboarding"
    shutil.copytree(source_dir, target_dir)
    return target_dir / "example_manifest.yml"


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


def test_bootstrap_sqlite_store_creates_expected_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"

    bootstrap_sqlite_store(db_path)

    with closing(sqlite3.connect(db_path)) as connection:
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert set(PIPELINE_STATE_TABLES) <= names


def test_run_all_persists_and_reload_state_from_sqlite(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    run_id = store.latest_run_id()
    assert run_id is not None

    bundle = store.load_run_bundle(run_id)
    summary = json.loads((base_dir / "data" / "exceptions" / "run_summary.json").read_text(encoding="utf-8"))

    assert bundle.run.status == "completed"
    assert bundle.run.input_mode == "synthetic"
    assert bundle.run.batch_id == "synthetic:small:42"
    assert bundle.run.total_records == summary["total_records"]
    assert bundle.run.candidate_pair_count == summary["candidate_pair_count"]
    assert bundle.run.cluster_count == summary["cluster_count"]
    assert bundle.run.golden_record_count == summary["golden_record_count"]
    assert bundle.run.review_queue_count == summary["review_queue_count"]
    assert bundle.run.summary == summary

    assert bundle.normalized_rows == _read_csv_rows(
        base_dir / "data" / "normalized" / "normalized_person_records.csv"
    )
    assert bundle.candidate_pairs == _read_csv_rows(
        base_dir / "data" / "matches" / "candidate_scores.csv"
    )
    assert bundle.blocking_metrics_rows == _read_csv_rows(
        base_dir / "data" / "matches" / "blocking_metrics.csv"
    )
    assert bundle.cluster_rows == _read_csv_rows(
        base_dir / "data" / "matches" / "entity_clusters.csv"
    )
    assert bundle.golden_rows == _read_csv_rows(
        base_dir / "data" / "golden" / "golden_person_records.csv"
    )
    assert bundle.crosswalk_rows == _read_csv_rows(
        base_dir / "data" / "golden" / "source_to_golden_crosswalk.csv"
    )
    assert bundle.review_rows == _read_csv_rows(
        base_dir / "data" / "review_queue" / "manual_review_queue.csv"
    )

    reloaded_report = tmp_path / "reloaded" / "run_report.md"
    assert (
        main(
            [
                "report",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--output",
                str(reloaded_report),
            ]
        )
        == 0
    )

    reloaded_summary = json.loads(
        reloaded_report.with_name("run_summary.json").read_text(encoding="utf-8")
    )
    reloaded_report_text = reloaded_report.read_text(encoding="utf-8")

    assert reloaded_summary == summary
    assert f"state-db://{db_path.name}?run_id={run_id}" in reloaded_report_text


def test_run_all_persists_public_safety_activity_from_manifest_source_bundles(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    manifest_path = _copy_public_safety_onboarding_fixture(tmp_path / "public_safety_onboarding")

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
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    run_id = store.latest_run_id()
    assert run_id is not None

    bundle = store.load_run_bundle(run_id)
    incident_identity_path = base_dir / "data" / "public_safety_demo" / "incident_identity_view.csv"
    golden_activity_path = base_dir / "data" / "public_safety_demo" / "golden_person_activity.csv"

    assert bundle.public_safety_incident_identity_rows == _read_csv_rows(incident_identity_path)
    assert bundle.public_safety_golden_activity_rows == _read_csv_rows(golden_activity_path)
    assert bundle.run.summary["public_safety_activity"]["incident_count"] >= 1
    assert bundle.run.summary["public_safety_activity"]["cross_system_golden_person_count"] >= 1

    shutil.rmtree(base_dir / "data")

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
            ]
        )
        == 0
    )

    assert _read_csv_rows(incident_identity_path) == bundle.public_safety_incident_identity_rows
    assert _read_csv_rows(golden_activity_path) == bundle.public_safety_golden_activity_rows


def test_store_operational_metrics_and_audit_events_reflect_persisted_batch_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    run_id = store.latest_completed_run_id()
    assert run_id is not None

    audit_event = store.record_audit_event(
        actor_type="cli",
        actor_id="operator",
        action="publish_run",
        resource_type="pipeline_run",
        resource_id=run_id,
        run_id=run_id,
        status="succeeded",
        details={"snapshot_dir": str(tmp_path / "published")},
    )
    metrics = store.load_operational_metrics()
    listed_events = store.list_audit_events(run_id=run_id)

    assert audit_event.action == "publish_run"
    assert audit_event.run_id == run_id
    assert audit_event.details["snapshot_dir"] == str(tmp_path / "published")
    assert metrics.run_status_counts["completed"] == 1
    assert metrics.run_status_counts["running"] == 0
    assert metrics.export_status_counts["completed"] == 0
    assert metrics.review_case_status_counts["pending"] >= 0
    assert metrics.audit_event_count == 1
    assert metrics.latest_completed_run_id == run_id
    assert listed_events[0].audit_event_id == audit_event.audit_event_id


def test_store_scopes_runs_and_audit_events_by_tenant(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    first_base_dir = tmp_path / "tenant_a_run"
    second_base_dir = tmp_path / "tenant_b_run"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(first_base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
                "--tenant-id",
                "tenant-a",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(second_base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
                "--tenant-id",
                "tenant-b",
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    tenant_a_run_id = store.latest_completed_run_id(tenant_id="tenant-a")
    tenant_b_run_id = store.latest_completed_run_id(tenant_id="tenant-b")

    assert tenant_a_run_id is not None
    assert tenant_b_run_id is not None
    assert tenant_a_run_id != tenant_b_run_id
    assert store.latest_completed_run_id() is None
    assert store.load_run_record(tenant_a_run_id).tenant_id == "tenant-a"
    assert store.load_run_record(tenant_b_run_id).tenant_id == "tenant-b"
    assert store.list_run_records(tenant_id="tenant-a", limit=10).total_count == 1
    assert store.list_run_records(tenant_id="tenant-b", limit=10).total_count == 1

    tenant_a_audit = store.record_audit_event(
        actor_type="cli",
        actor_id="operator",
        action="publish_run",
        resource_type="pipeline_run",
        resource_id=tenant_a_run_id,
        run_id=tenant_a_run_id,
        status="succeeded",
        details={"snapshot_dir": str(tmp_path / "published" / "tenant-a")},
    )
    tenant_b_audit = store.record_audit_event(
        tenant_id="tenant-b",
        actor_type="cli",
        actor_id="operator",
        action="publish_run",
        resource_type="pipeline_run",
        resource_id=tenant_b_run_id,
        status="succeeded",
        details={"snapshot_dir": str(tmp_path / "published" / "tenant-b")},
    )

    tenant_a_metrics = store.load_operational_metrics(tenant_id="tenant-a")
    tenant_b_metrics = store.load_operational_metrics(tenant_id="tenant-b")
    tenant_a_events = store.list_audit_events(tenant_id="tenant-a", limit=10)
    tenant_b_events = store.list_audit_events(tenant_id="tenant-b", limit=10)

    assert tenant_a_audit.tenant_id == "tenant-a"
    assert tenant_b_audit.tenant_id == "tenant-b"
    assert tenant_a_metrics.latest_completed_run_id == tenant_a_run_id
    assert tenant_b_metrics.latest_completed_run_id == tenant_b_run_id
    assert tenant_a_metrics.audit_event_count == 1
    assert tenant_b_metrics.audit_event_count == 1
    assert [event.tenant_id for event in tenant_a_events] == ["tenant-a"]
    assert [event.tenant_id for event in tenant_b_events] == ["tenant-b"]


def test_record_audit_event_redacts_free_text_and_auth_material(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    store = SQLitePipelineStore(db_path)

    audit_event = store.record_audit_event(
        actor_type="service_api",
        actor_id="review.operator",
        action="apply_review_decision",
        resource_type="review_case",
        resource_id="REV-001",
        status="succeeded",
        details={
            "notes": "Approved John Doe after manual CJIS-side verification",
            "authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
            "nested": {
                "token": "eyJhbGciOiJIUzI1NiJ9.payload.signature",
                "error": (
                    "Replay failed for postgresql://svc_user:super-secret-password@db.internal/identity "
                    "with Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature"
                ),
            },
        },
    )

    assert audit_event.details["notes"].startswith("[REDACTED free_text len=")
    assert audit_event.details["authorization"] == "[REDACTED auth_material]"
    assert audit_event.details["nested"]["token"] == "[REDACTED auth_material]"
    assert "super-secret-password" not in audit_event.details["nested"]["error"]
    assert "payload.signature" not in audit_event.details["nested"]["error"]
    assert "Bearer [REDACTED]" in audit_event.details["nested"]["error"]


def test_run_all_reuses_completed_run_without_duplicating_persisted_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )

    first_store = SQLitePipelineStore(db_path)
    first_run_id = first_store.latest_run_id()
    assert first_run_id is not None

    # Remove emitted artifacts to prove the second invocation restores them from persisted state.
    for child in (base_dir / "data").iterdir():
        if child.is_dir():
            for nested in child.rglob("*"):
                if nested.is_file():
                    nested.unlink()
            for nested in sorted(child.rglob("*"), reverse=True):
                if nested.is_dir():
                    nested.rmdir()
            child.rmdir()

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )

    run_rows = _read_pipeline_runs(db_path)
    assert len(run_rows) == 1
    assert run_rows[0]["run_id"] == first_run_id
    assert run_rows[0]["status"] == "completed"
    assert (base_dir / "data" / "normalized" / "normalized_person_records.csv").exists()
    assert (base_dir / "data" / "exceptions" / "run_report.md").exists()


def test_run_all_resumes_failed_manifest_run_from_latest_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="resume-checkpoint-001",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )
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

    normalize_calls = 0
    match_calls = 0
    original_normalize = cli_module._write_normalized_output
    original_match = cli_module._write_match_output
    original_quality = cli_module._write_quality_outputs

    def instrumented_normalize(*args, **kwargs):
        nonlocal normalize_calls
        normalize_calls += 1
        return original_normalize(*args, **kwargs)

    def instrumented_match(*args, **kwargs):
        nonlocal match_calls
        match_calls += 1
        return original_match(*args, **kwargs)

    quality_calls = 0

    def fail_once_quality(*args, **kwargs):
        nonlocal quality_calls
        quality_calls += 1
        if quality_calls == 1:
            raise RuntimeError("injected report-stage failure")
        return original_quality(*args, **kwargs)

    monkeypatch.setattr(cli_module, "_write_normalized_output", instrumented_normalize)
    monkeypatch.setattr(cli_module, "_write_match_output", instrumented_match)
    monkeypatch.setattr(cli_module, "_write_quality_outputs", fail_once_quality)

    with pytest.raises(RuntimeError, match="injected report-stage failure"):
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

    store = SQLitePipelineStore(db_path)
    failed_run_rows = _read_pipeline_runs(db_path)
    assert len(failed_run_rows) == 1
    failed_run_id = str(failed_run_rows[0]["run_id"])
    failed_run = store.load_run_record(failed_run_id)
    failed_checkpoint = store.load_latest_run_checkpoint(failed_run_id)

    assert failed_run.status == "failed"
    assert failed_run.attempt_number == 1
    assert failed_run.summary["resume"]["resume_supported"] is True
    assert failed_run.summary["resume"]["available_checkpoint_stage"] == "crosswalk"
    assert failed_checkpoint is not None
    assert failed_checkpoint.stage_name == "crosswalk"

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

    completed_run_rows = _read_pipeline_runs(db_path)
    assert len(completed_run_rows) == 2
    completed_run = store.latest_completed_run_for_run_key(failed_run.run_key)
    assert completed_run is not None
    assert completed_run.attempt_number == 2
    assert completed_run.resumed_from_run_id == failed_run_id
    assert completed_run.summary["resume"]["resumed"] is True
    assert completed_run.summary["resume"]["resumed_from_run_id"] == failed_run_id
    assert completed_run.summary["resume"]["resumed_from_stage"] == "crosswalk"

    assert normalize_calls == 1
    assert match_calls == 1
    assert quality_calls == 2
    assert (base_dir / "data" / "exceptions" / "run_report.md").exists()
    assert (base_dir / "data" / "golden" / "source_to_golden_crosswalk.csv").exists()


def test_persisted_review_case_workflow_supports_assignment_and_lifecycle_transitions(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )

    store = SQLitePipelineStore(db_path)
    run_id = store.latest_completed_run_id_with_review_cases()
    assert run_id is not None
    capsys.readouterr()

    initial_cases = store.list_review_cases(run_id=run_id)
    assert initial_cases
    case = initial_cases[0]
    assert case.queue_status == "pending"
    assert case.assigned_to == ""
    assert case.operator_notes == ""
    assert case.created_at_utc
    assert case.updated_at_utc == case.created_at_utc
    assert case.resolved_at_utc == ""

    assert (
        main(
            [
                "review-case-update",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--review-id",
                case.review_id,
                "--assigned-to",
                "analyst.one",
                "--notes",
                "Need source verification",
                "--status",
                "deferred",
            ]
        )
        == 0
    )
    updated_payload = json.loads(capsys.readouterr().out)
    assert updated_payload["queue_status"] == "deferred"
    assert updated_payload["assigned_to"] == "analyst.one"
    assert updated_payload["operator_notes"] == "Need source verification"
    assert updated_payload["resolved_at_utc"] == ""

    assert (
        main(
            [
                "review-case-update",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--review-id",
                case.review_id,
                "--status",
                "approved",
                "--notes",
                "Approved after verification",
            ]
        )
        == 0
    )
    approved_payload = json.loads(capsys.readouterr().out)
    assert approved_payload["queue_status"] == "approved"
    assert approved_payload["assigned_to"] == "analyst.one"
    assert approved_payload["operator_notes"] == "Approved after verification"
    assert approved_payload["resolved_at_utc"]

    approved_case = store.load_review_case(run_id=run_id, review_id=case.review_id)
    assert approved_case.queue_status == "approved"
    assert approved_case.assigned_to == "analyst.one"
    assert approved_case.operator_notes == "Approved after verification"
    assert approved_case.resolved_at_utc

    assert (
        main(
            [
                "review-case-list",
                "--state-db",
                str(db_path),
                "--status",
                "approved",
            ]
        )
        == 0
    )
    listed_payload = json.loads(capsys.readouterr().out)
    assert any(item["review_id"] == case.review_id for item in listed_payload)

    with pytest.raises(
        ValueError,
        match=r"Review case cannot transition from 'approved' to 'deferred'",
    ):
        main(
            [
                "review-case-update",
                "--state-db",
                str(db_path),
                "--run-id",
                run_id,
                "--review-id",
                case.review_id,
                "--status",
                "deferred",
            ]
        )

    for child in (base_dir / "data").iterdir():
        if child.is_dir():
            for nested in child.rglob("*"):
                if nested.is_file():
                    nested.unlink()
            for nested in sorted(child.rglob("*"), reverse=True):
                if nested.is_dir():
                    nested.rmdir()
            child.rmdir()

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
            ]
        )
        == 0
    )
    restored_review_rows = _read_csv_rows(base_dir / "data" / "review_queue" / "manual_review_queue.csv")
    assert all(row["review_id"] != case.review_id for row in restored_review_rows)


def test_run_all_records_failed_attempt_and_allows_clean_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    config_dir = tmp_path / "config"
    _write_config_copy(config_dir)

    (config_dir / "thresholds.yml").write_text(
        """
not_thresholds:
  auto_merge: 0.95
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"thresholds\.yml: top-level config contains unsupported keys: not_thresholds"):
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
                "--config-dir",
                str(config_dir),
            ]
        )

    failed_runs = _read_pipeline_runs(db_path)
    assert len(failed_runs) == 1
    assert failed_runs[0]["status"] == "failed"
    assert "thresholds.yml" in str(failed_runs[0]["failure_detail"])
    assert failed_runs[0]["attempt_number"] == 1

    _write_config_copy(config_dir)

    assert (
        main(
            [
                "run-all",
                "--base-dir",
                str(base_dir),
                "--profile",
                "small",
                "--seed",
                "42",
                "--state-db",
                str(db_path),
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )

    run_rows = _read_pipeline_runs(db_path)
    assert len(run_rows) == 2
    assert run_rows[0]["status"] == "failed"
    assert run_rows[1]["status"] == "completed"
    assert run_rows[0]["run_key"] == run_rows[1]["run_key"]
    assert run_rows[1]["attempt_number"] == 2
    assert run_rows[1]["failure_detail"] in (None, "")


def test_incremental_manifest_refresh_reuses_unaffected_entities(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="inbound-2026-03-13",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )

    batch_one_source_a = [
        _person_row(
            source_record_id="A-1",
            person_entity_id="P-1",
            source_system="source_a",
            first_name="John",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        ),
        _person_row(
            source_record_id="A-2",
            person_entity_id="P-2",
            source_system="source_a",
            first_name="Jane",
            last_name="Doe",
            dob="1990-05-20",
            address="20 Oak St",
            phone="5552222222",
        ),
    ]
    batch_one_source_b = [
        _person_row(
            source_record_id="B-1",
            person_entity_id="P-1",
            source_system="source_b",
            first_name="Jon",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main Street",
            phone="5551111111",
        ),
        _person_row(
            source_record_id="B-2",
            person_entity_id="P-2",
            source_system="source_b",
            first_name="Jane",
            last_name="Doe",
            dob="1990-05-20",
            address="20 Oak Street Apt 2",
            phone="5552222222",
        ),
    ]
    _write_csv_rows(landing_dir / "agency_a.csv", batch_one_source_a)
    _write_parquet_rows(landing_dir / "agency_b.parquet", batch_one_source_b)

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
    first_run_id = store.latest_run_id()
    assert first_run_id is not None
    first_bundle = store.load_run_bundle(first_run_id)
    first_crosswalk_by_record = {
        row["source_record_id"]: row["golden_id"] for row in first_bundle.crosswalk_rows
    }
    first_cluster_by_record = {
        row["source_record_id"]: row["cluster_id"] for row in first_bundle.cluster_rows
    }
    first_golden_by_id = {
        row["golden_id"]: row for row in first_bundle.golden_rows
    }

    _write_manifest(
        manifest_path,
        batch_id="inbound-2026-03-14",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )
    batch_two_source_a = [dict(row) for row in batch_one_source_a]
    batch_two_source_a[1]["address"] = "99 Elm Street"
    batch_two_source_b = [dict(row) for row in batch_one_source_b]
    _write_csv_rows(landing_dir / "agency_a.csv", batch_two_source_a)
    _write_parquet_rows(landing_dir / "agency_b.parquet", batch_two_source_b)

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
                "incremental",
            ]
        )
        == 0
    )

    second_run_id = store.latest_run_id()
    assert second_run_id is not None
    assert second_run_id != first_run_id

    second_bundle = store.load_run_bundle(second_run_id)
    refresh = second_bundle.run.summary["refresh"]
    run_context = second_bundle.run.summary["run_context"]
    second_crosswalk_by_record = {
        row["source_record_id"]: row["golden_id"] for row in second_bundle.crosswalk_rows
    }
    second_cluster_by_record = {
        row["source_record_id"]: row["cluster_id"] for row in second_bundle.cluster_rows
    }
    second_golden_by_id = {
        row["golden_id"]: row for row in second_bundle.golden_rows
    }

    assert run_context["refresh_mode"] == "incremental"
    assert refresh["mode"] == "incremental"
    assert refresh["fallback_to_full"] is False
    assert refresh["predecessor_run_id"] == first_run_id
    assert refresh["changed_record_count"] == 1
    assert refresh["inserted_record_count"] == 0
    assert refresh["removed_record_count"] == 0
    assert refresh["affected_record_count"] == 2
    assert refresh["reused_record_count"] == 2
    assert refresh["recalculated_candidate_pair_count"] == 1
    assert refresh["reused_candidate_pair_count"] == 1
    assert refresh["recalculated_cluster_count"] == 1
    assert refresh["reused_cluster_count"] == 2

    assert second_cluster_by_record["A-1"] == first_cluster_by_record["A-1"]
    assert second_cluster_by_record["B-1"] == first_cluster_by_record["B-1"]
    assert second_crosswalk_by_record["A-1"] == first_crosswalk_by_record["A-1"]
    assert second_crosswalk_by_record["B-1"] == first_crosswalk_by_record["B-1"]
    assert (
        second_golden_by_id[second_crosswalk_by_record["A-1"]]["address"]
        == first_golden_by_id[first_crosswalk_by_record["A-1"]]["address"]
    )
    assert second_golden_by_id[second_crosswalk_by_record["A-2"]]["address"] == "99 Elm Street"


def test_approved_review_case_forces_merge_and_replays_across_manifest_reruns(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="review-approve-001",
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
    first_run_id = store.latest_run_id()
    assert first_run_id is not None
    first_bundle = store.load_run_bundle(first_run_id)
    first_match = first_bundle.candidate_pairs[0]
    assert first_match["decision"] == "manual_review"
    assert len(first_bundle.golden_rows) == 2

    review_case = store.list_review_cases(run_id=first_run_id)[0]
    updated_case = store.update_review_case(
        run_id=first_run_id,
        review_id=review_case.review_id,
        queue_status="approved",
        operator_notes="Approved by analyst",
        updated_at_utc="2026-03-14T03:10:00Z",
    )
    assert updated_case.queue_status == "approved"

    _write_manifest(
        manifest_path,
        batch_id="review-approve-002",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )
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
                "incremental",
            ]
        )
        == 0
    )

    second_run_id = store.latest_run_id()
    assert second_run_id is not None and second_run_id != first_run_id
    second_bundle = store.load_run_bundle(second_run_id)
    second_match = second_bundle.candidate_pairs[0]
    second_summary = second_bundle.run.summary

    assert second_match["decision"] == "auto_merge"
    assert "review_case_approved_override" in second_match["reason_trace"]
    assert len(second_bundle.golden_rows) == 1
    assert second_summary["review_queue_count"] == 0
    assert second_summary["cluster_count"] == 1
    assert second_bundle.review_rows[0]["queue_status"] == "approved"

    _write_manifest(
        manifest_path,
        batch_id="review-approve-003",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )
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
                "incremental",
            ]
        )
        == 0
    )

    third_run_id = store.latest_run_id()
    assert third_run_id is not None and third_run_id != second_run_id
    third_bundle = store.load_run_bundle(third_run_id)
    refresh = third_bundle.run.summary["refresh"]
    assert third_bundle.candidate_pairs[0]["decision"] == "auto_merge"
    assert len(third_bundle.golden_rows) == 1
    assert refresh["affected_record_count"] == 0
    assert refresh["reused_candidate_pair_count"] == 1


def test_rejected_review_case_blocks_future_auto_merge_on_manifest_rerun(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "pipeline_state.sqlite"
    base_dir = tmp_path / "run"
    landing_dir = tmp_path / "landing"
    manifest_path = _write_manifest(
        tmp_path / "manifest.yml",
        batch_id="review-reject-001",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )

    initial_source_a_rows = [
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
    initial_source_b_rows = [
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
    _write_csv_rows(landing_dir / "agency_a.csv", initial_source_a_rows)
    _write_parquet_rows(landing_dir / "agency_b.parquet", initial_source_b_rows)

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
    first_run_id = store.latest_run_id()
    assert first_run_id is not None
    review_case = store.list_review_cases(run_id=first_run_id)[0]
    updated_case = store.update_review_case(
        run_id=first_run_id,
        review_id=review_case.review_id,
        queue_status="rejected",
        operator_notes="Do not merge",
        updated_at_utc="2026-03-14T03:15:00Z",
    )
    assert updated_case.queue_status == "rejected"

    updated_source_b_rows = [
        _person_row(
            source_record_id="B-1",
            person_entity_id="P-2",
            source_system="source_b",
            first_name="John",
            last_name="Smith",
            dob="1985-03-12",
            address="123 Main St",
            phone="5551111111",
        )
    ]
    _write_manifest(
        manifest_path,
        batch_id="review-reject-002",
        source_a_path="agency_a.csv",
        source_b_path="agency_b.parquet",
    )
    _write_csv_rows(landing_dir / "agency_a.csv", initial_source_a_rows)
    _write_parquet_rows(landing_dir / "agency_b.parquet", updated_source_b_rows)

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
                "incremental",
            ]
        )
        == 0
    )

    second_run_id = store.latest_run_id()
    assert second_run_id is not None and second_run_id != first_run_id
    second_bundle = store.load_run_bundle(second_run_id)
    second_match = second_bundle.candidate_pairs[0]

    assert second_match["score"] == "1.0" or float(second_match["score"]) == 1.0
    assert second_match["decision"] == "no_match"
    assert "review_case_rejected_override" in second_match["reason_trace"]
    assert len(second_bundle.golden_rows) == 2
    assert second_bundle.run.summary["review_queue_count"] == 0
    assert second_bundle.review_rows[0]["queue_status"] == "rejected"
