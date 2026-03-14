from __future__ import annotations

import csv
import json
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path

from django.db import transaction

from etl_identity_engine.demo_shell.models import (
    DemoRun,
    DemoScenario,
    GoldenPersonActivity,
    IncidentIdentity,
)


SUMMARY_RELATIVE_PATH = Path("data/public_safety_demo/public_safety_demo_summary.json")
SCENARIOS_RELATIVE_PATH = Path("data/public_safety_demo/public_safety_demo_scenarios.json")
INCIDENT_IDENTITY_RELATIVE_PATH = Path("data/public_safety_demo/incident_identity_view.csv")
GOLDEN_ACTIVITY_RELATIVE_PATH = Path("data/public_safety_demo/golden_person_activity.csv")
MANIFEST_RELATIVE_PATH = Path("demo_manifest.json")


@dataclass(frozen=True)
class LoadedDemoBundle:
    demo_run: DemoRun
    bundle_path: Path
    bundle_root: Path


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> dict[str, object] | list[object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_bundle(bundle_path: Path, extract_dir: Path) -> None:
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle_path) as archive:
        archive.extractall(extract_dir)
    shutil.copy2(bundle_path, extract_dir / bundle_path.name)


def _parse_int(value: str | int | None) -> int:
    if value in (None, ""):
        return 0
    return int(value)


@transaction.atomic
def load_public_safety_demo_bundle(*, bundle_path: Path, extract_dir: Path) -> LoadedDemoBundle:
    resolved_bundle_path = bundle_path.resolve()
    resolved_extract_dir = extract_dir.resolve()
    _extract_bundle(resolved_bundle_path, resolved_extract_dir)

    manifest = _read_json(resolved_extract_dir / MANIFEST_RELATIVE_PATH)
    summary = _read_json(resolved_extract_dir / SUMMARY_RELATIVE_PATH)
    scenarios = _read_json(resolved_extract_dir / SCENARIOS_RELATIVE_PATH)
    incident_identity_rows = _read_csv_rows(resolved_extract_dir / INCIDENT_IDENTITY_RELATIVE_PATH)
    golden_activity_rows = _read_csv_rows(resolved_extract_dir / GOLDEN_ACTIVITY_RELATIVE_PATH)

    DemoRun.objects.all().delete()

    if not isinstance(manifest, dict):
        raise ValueError("demo manifest must be a JSON object")
    if not isinstance(summary, dict):
        raise ValueError("public safety summary must be a JSON object")
    if not isinstance(scenarios, list):
        raise ValueError("public safety scenarios must be a JSON list")

    demo_run = DemoRun.objects.create(
        bundle_name=resolved_bundle_path.name,
        bundle_path=str(resolved_bundle_path),
        bundle_root=str(resolved_extract_dir),
        version=str(manifest.get("version", "")),
        profile=str(manifest.get("profile", "")),
        seed=_parse_int(manifest.get("seed")),
        formats=list(manifest.get("formats", [])),
        generated_at_utc=str(manifest.get("generated_at_utc", "")),
        source_commit=str(manifest.get("source_commit", "")),
        artifact_paths=list(manifest.get("artifacts", [])),
        summary=summary,
        top_golden_people_by_activity=list(summary.get("top_golden_people_by_activity", [])),
        incident_count=_parse_int(summary.get("incident_count")),
        incident_person_link_count=_parse_int(summary.get("incident_person_link_count")),
        cad_incident_count=_parse_int(summary.get("cad_incident_count")),
        rms_incident_count=_parse_int(summary.get("rms_incident_count")),
        resolved_link_count=_parse_int(summary.get("resolved_link_count")),
        unresolved_link_count=_parse_int(summary.get("unresolved_link_count")),
        linked_golden_person_count=_parse_int(summary.get("linked_golden_person_count")),
        cross_system_golden_person_count=_parse_int(summary.get("cross_system_golden_person_count")),
    )

    DemoScenario.objects.bulk_create(
        [
            DemoScenario(
                demo_run=demo_run,
                scenario_id=str(item.get("scenario_id", "")),
                title=str(item.get("title", "")),
                golden_id=str(item.get("golden_id", "")),
                golden_name=str(item.get("golden_name", "")),
                narrative=str(item.get("narrative", "")),
                cad_incident_count=_parse_int(item.get("cad_incident_count")),
                rms_incident_count=_parse_int(item.get("rms_incident_count")),
                total_incident_count=_parse_int(item.get("total_incident_count")),
                latest_incident_at=str(item.get("latest_incident_at", "")),
            )
            for item in scenarios
            if isinstance(item, dict)
        ]
    )

    GoldenPersonActivity.objects.bulk_create(
        [
            GoldenPersonActivity(
                demo_run=demo_run,
                golden_id=str(row.get("golden_id", "")),
                cluster_id=str(row.get("cluster_id", "")),
                person_entity_id=str(row.get("person_entity_id", "")),
                golden_first_name=str(row.get("golden_first_name", "")),
                golden_last_name=str(row.get("golden_last_name", "")),
                cad_incident_count=_parse_int(row.get("cad_incident_count")),
                rms_incident_count=_parse_int(row.get("rms_incident_count")),
                total_incident_count=_parse_int(row.get("total_incident_count")),
                linked_source_record_count=_parse_int(row.get("linked_source_record_count")),
                roles=str(row.get("roles", "")),
                latest_incident_at=str(row.get("latest_incident_at", "")),
            )
            for row in golden_activity_rows
        ]
    )

    IncidentIdentity.objects.bulk_create(
        [
            IncidentIdentity(
                demo_run=demo_run,
                incident_id=str(row.get("incident_id", "")),
                incident_source_system=str(row.get("incident_source_system", "")),
                occurred_at=str(row.get("occurred_at", "")),
                incident_location=str(row.get("incident_location", "")),
                incident_city=str(row.get("incident_city", "")),
                incident_state=str(row.get("incident_state", "")),
                incident_role=str(row.get("incident_role", "")),
                person_entity_id=str(row.get("person_entity_id", "")),
                source_record_id=str(row.get("source_record_id", "")),
                person_source_system=str(row.get("person_source_system", "")),
                golden_id=str(row.get("golden_id", "")),
                cluster_id=str(row.get("cluster_id", "")),
                golden_first_name=str(row.get("golden_first_name", "")),
                golden_last_name=str(row.get("golden_last_name", "")),
                golden_dob=str(row.get("golden_dob", "")),
                golden_address=str(row.get("golden_address", "")),
                golden_phone=str(row.get("golden_phone", "")),
            )
            for row in incident_identity_rows
        ]
    )

    return LoadedDemoBundle(
        demo_run=demo_run,
        bundle_path=resolved_bundle_path,
        bundle_root=resolved_extract_dir,
    )
