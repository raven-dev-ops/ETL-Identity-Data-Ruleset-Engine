from __future__ import annotations

import csv
import json
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path

from django.db import transaction

from etl_identity_engine import __version__
from etl_identity_engine.demo_shell.models import (
    DemoRun,
    DemoScenario,
    GoldenPersonActivity,
    IncidentIdentity,
)
from etl_identity_engine.io.write import write_csv_dicts, write_markdown
from etl_identity_engine.output_contracts import (
    PUBLIC_SAFETY_GOLDEN_ACTIVITY_HEADERS,
    PUBLIC_SAFETY_INCIDENT_IDENTITY_HEADERS,
)
from etl_identity_engine.public_safety_demo import (
    build_public_safety_demo_dashboard_html,
    build_public_safety_demo_report_markdown,
    build_public_safety_demo_walkthrough_markdown,
)
from etl_identity_engine.storage.sqlite_store import PipelineStateStore


SUMMARY_RELATIVE_PATH = Path("data/public_safety_demo/public_safety_demo_summary.json")
SCENARIOS_RELATIVE_PATH = Path("data/public_safety_demo/public_safety_demo_scenarios.json")
INCIDENT_IDENTITY_RELATIVE_PATH = Path("data/public_safety_demo/incident_identity_view.csv")
GOLDEN_ACTIVITY_RELATIVE_PATH = Path("data/public_safety_demo/golden_person_activity.csv")
MANIFEST_RELATIVE_PATH = Path("demo_manifest.json")
REPORT_RELATIVE_PATH = Path("data/public_safety_demo/public_safety_demo_report.md")
WALKTHROUGH_RELATIVE_PATH = Path("data/public_safety_demo/public_safety_demo_walkthrough.md")
DASHBOARD_RELATIVE_PATH = Path("data/public_safety_demo/public_safety_demo_dashboard.html")
STATE_SOURCE_RELATIVE_PATH = Path("persisted_run_source.json")


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


def _top_golden_people_from_activity_rows(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            -_parse_int(row.get("total_incident_count")),
            str(row.get("golden_last_name", "")),
            str(row.get("golden_first_name", "")),
            str(row.get("golden_id", "")),
        ),
    )
    return [
        {
            "golden_id": str(row.get("golden_id", "")),
            "golden_name": " ".join(
                part
                for part in (
                    str(row.get("golden_first_name", "")).strip(),
                    str(row.get("golden_last_name", "")).strip(),
                )
                if part
            ),
            "total_incident_count": _parse_int(row.get("total_incident_count")),
            "cad_incident_count": _parse_int(row.get("cad_incident_count")),
            "rms_incident_count": _parse_int(row.get("rms_incident_count")),
        }
        for row in sorted_rows[:5]
    ]


def _ensure_public_safety_summary(
    summary: dict[str, object],
    *,
    incident_identity_rows: list[dict[str, str]],
    golden_activity_rows: list[dict[str, str]],
) -> dict[str, object]:
    resolved_summary = dict(summary)
    incident_ids = {
        str(row.get("incident_id", "")).strip()
        for row in incident_identity_rows
        if str(row.get("incident_id", "")).strip()
    }
    cad_ids = {
        str(row.get("incident_id", "")).strip()
        for row in incident_identity_rows
        if str(row.get("incident_source_system", "")).strip().lower() == "cad"
        and str(row.get("incident_id", "")).strip()
    }
    rms_ids = {
        str(row.get("incident_id", "")).strip()
        for row in incident_identity_rows
        if str(row.get("incident_source_system", "")).strip().lower() == "rms"
        and str(row.get("incident_id", "")).strip()
    }
    resolved_link_count = sum(1 for row in incident_identity_rows if str(row.get("golden_id", "")).strip())
    unresolved_link_count = len(incident_identity_rows) - resolved_link_count
    cross_system_count = sum(
        1
        for row in golden_activity_rows
        if _parse_int(row.get("cad_incident_count")) > 0 and _parse_int(row.get("rms_incident_count")) > 0
    )

    resolved_summary.setdefault("incident_count", len(incident_ids))
    resolved_summary.setdefault("incident_person_link_count", len(incident_identity_rows))
    resolved_summary.setdefault("cad_incident_count", len(cad_ids))
    resolved_summary.setdefault("rms_incident_count", len(rms_ids))
    resolved_summary.setdefault("resolved_link_count", resolved_link_count)
    resolved_summary.setdefault("unresolved_link_count", unresolved_link_count)
    resolved_summary.setdefault("linked_golden_person_count", len(golden_activity_rows))
    resolved_summary.setdefault("cross_system_golden_person_count", cross_system_count)
    resolved_summary.setdefault(
        "top_golden_people_by_activity",
        _top_golden_people_from_activity_rows(golden_activity_rows),
    )
    resolved_summary.setdefault("demo_scenarios", [])
    return resolved_summary


def _persist_demo_shell_rows(
    *,
    bundle_name: str,
    bundle_path: str,
    bundle_root: Path,
    manifest: dict[str, object],
    summary: dict[str, object],
    scenarios: list[object],
    incident_identity_rows: list[dict[str, str]],
    golden_activity_rows: list[dict[str, str]],
) -> DemoRun:
    DemoRun.objects.all().delete()

    if not isinstance(manifest, dict):
        raise ValueError("demo manifest must be a JSON object")
    if not isinstance(summary, dict):
        raise ValueError("public safety summary must be a JSON object")
    if not isinstance(scenarios, list):
        raise ValueError("public safety scenarios must be a JSON list")

    demo_run = DemoRun.objects.create(
        bundle_name=bundle_name,
        bundle_path=bundle_path,
        bundle_root=str(bundle_root),
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
    return demo_run


def _write_public_safety_demo_artifacts(
    *,
    bundle_root: Path,
    manifest: dict[str, object],
    summary: dict[str, object],
    incident_identity_rows: list[dict[str, str]],
    golden_activity_rows: list[dict[str, str]],
) -> None:
    scenarios = summary.get("demo_scenarios", [])
    bundle_root.mkdir(parents=True, exist_ok=True)
    (bundle_root / MANIFEST_RELATIVE_PATH).parent.mkdir(parents=True, exist_ok=True)
    (bundle_root / SUMMARY_RELATIVE_PATH).parent.mkdir(parents=True, exist_ok=True)
    (bundle_root / SCENARIOS_RELATIVE_PATH).parent.mkdir(parents=True, exist_ok=True)
    (bundle_root / MANIFEST_RELATIVE_PATH).write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (bundle_root / SUMMARY_RELATIVE_PATH).write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (bundle_root / SCENARIOS_RELATIVE_PATH).write_text(
        json.dumps(scenarios, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_csv_dicts(
        bundle_root / INCIDENT_IDENTITY_RELATIVE_PATH,
        incident_identity_rows,
        fieldnames=PUBLIC_SAFETY_INCIDENT_IDENTITY_HEADERS,
    )
    write_csv_dicts(
        bundle_root / GOLDEN_ACTIVITY_RELATIVE_PATH,
        golden_activity_rows,
        fieldnames=PUBLIC_SAFETY_GOLDEN_ACTIVITY_HEADERS,
    )
    write_markdown(
        bundle_root / REPORT_RELATIVE_PATH,
        build_public_safety_demo_report_markdown(summary),
    )
    write_markdown(
        bundle_root / WALKTHROUGH_RELATIVE_PATH,
        build_public_safety_demo_walkthrough_markdown(summary),
    )
    write_markdown(
        bundle_root / DASHBOARD_RELATIVE_PATH,
        build_public_safety_demo_dashboard_html(
            summary=summary,
            golden_person_activity_rows=golden_activity_rows,
            incident_identity_rows=incident_identity_rows,
        ),
    )


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

    demo_run = _persist_demo_shell_rows(
        bundle_name=resolved_bundle_path.name,
        bundle_path=str(resolved_bundle_path),
        bundle_root=resolved_extract_dir,
        manifest=manifest,
        summary=summary,
        scenarios=scenarios,
        incident_identity_rows=incident_identity_rows,
        golden_activity_rows=golden_activity_rows,
    )

    return LoadedDemoBundle(
        demo_run=demo_run,
        bundle_path=resolved_bundle_path,
        bundle_root=resolved_extract_dir,
    )


@transaction.atomic
def load_public_safety_demo_state(
    *,
    state_db: str | Path,
    extract_dir: Path,
    run_id: str | None = None,
) -> LoadedDemoBundle:
    resolved_extract_dir = extract_dir.resolve()
    if resolved_extract_dir.exists():
        shutil.rmtree(resolved_extract_dir)
    resolved_extract_dir.mkdir(parents=True, exist_ok=True)

    store = PipelineStateStore(state_db)
    try:
        resolved_run_id = run_id or store.latest_completed_run_id()
        if resolved_run_id is None:
            raise FileNotFoundError("No completed persisted run found for demo-shell loading")
        bundle = store.load_run_bundle(resolved_run_id)
    finally:
        store.engine.dispose()
    summary = _ensure_public_safety_summary(
        dict(bundle.run.summary.get("public_safety_activity") or {}),
        incident_identity_rows=bundle.public_safety_incident_identity_rows,
        golden_activity_rows=bundle.public_safety_golden_activity_rows,
    )
    summary.setdefault("source_run_id", resolved_run_id)
    summary.setdefault("source_mode", "persisted_state")
    summary.setdefault("source_state_db", str(state_db))
    summary.setdefault("generated_at_utc", bundle.run.finished_at_utc)

    manifest = {
        "bundle_type": "public_safety_demo_state",
        "version": __version__,
        "profile": bundle.run.profile or "",
        "seed": bundle.run.seed or 0,
        "formats": []
        if not bundle.run.formats
        else [item for item in bundle.run.formats.split(",") if item],
        "generated_at_utc": bundle.run.finished_at_utc,
        "source_commit": "",
        "source_run_id": resolved_run_id,
        "state_db": str(state_db),
        "artifacts": [
            str(INCIDENT_IDENTITY_RELATIVE_PATH).replace("\\", "/"),
            str(GOLDEN_ACTIVITY_RELATIVE_PATH).replace("\\", "/"),
            str(DASHBOARD_RELATIVE_PATH).replace("\\", "/"),
            str(REPORT_RELATIVE_PATH).replace("\\", "/"),
            str(SCENARIOS_RELATIVE_PATH).replace("\\", "/"),
            str(SUMMARY_RELATIVE_PATH).replace("\\", "/"),
            str(WALKTHROUGH_RELATIVE_PATH).replace("\\", "/"),
        ],
    }
    (resolved_extract_dir / STATE_SOURCE_RELATIVE_PATH).write_text(
        json.dumps(
            {
                "run_id": resolved_run_id,
                "state_db": str(state_db),
                "input_mode": bundle.run.input_mode,
                "base_dir": bundle.run.base_dir,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _write_public_safety_demo_artifacts(
        bundle_root=resolved_extract_dir,
        manifest=manifest,
        summary=summary,
        incident_identity_rows=bundle.public_safety_incident_identity_rows,
        golden_activity_rows=bundle.public_safety_golden_activity_rows,
    )
    demo_run = _persist_demo_shell_rows(
        bundle_name=f"persisted-run-{resolved_run_id}",
        bundle_path=str(resolved_extract_dir / STATE_SOURCE_RELATIVE_PATH),
        bundle_root=resolved_extract_dir,
        manifest=manifest,
        summary=summary,
        scenarios=list(summary.get("demo_scenarios", [])),
        incident_identity_rows=bundle.public_safety_incident_identity_rows,
        golden_activity_rows=bundle.public_safety_golden_activity_rows,
    )
    return LoadedDemoBundle(
        demo_run=demo_run,
        bundle_path=resolved_extract_dir / STATE_SOURCE_RELATIVE_PATH,
        bundle_root=resolved_extract_dir,
    )
