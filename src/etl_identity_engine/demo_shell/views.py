from __future__ import annotations

import mimetypes
from pathlib import Path

from django.conf import settings
from django.http import FileResponse, Http404, HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, render

from etl_identity_engine.demo_shell.models import DemoRun, DemoScenario, GoldenPersonActivity, IncidentIdentity


def _latest_demo_run() -> DemoRun | None:
    return DemoRun.objects.order_by("-loaded_at", "-id").first()


def _artifact_relative_paths(demo_run: DemoRun) -> dict[str, str]:
    return {
        "summary": "data/public_safety_demo/public_safety_demo_summary.json",
        "scenarios": "data/public_safety_demo/public_safety_demo_scenarios.json",
        "dashboard": "data/public_safety_demo/public_safety_demo_dashboard.html",
        "incident_identity": "data/public_safety_demo/incident_identity_view.csv",
        "golden_activity": "data/public_safety_demo/golden_person_activity.csv",
        "walkthrough": "data/public_safety_demo/public_safety_demo_walkthrough.md",
        "report": "data/public_safety_demo/public_safety_demo_report.md",
        "bundle_zip": demo_run.bundle_name,
    }


def index(request: HttpRequest) -> HttpResponse:
    demo_run = _latest_demo_run()
    if demo_run is None:
        return HttpResponse(
            "<h1>Public Safety Demo Shell</h1><p>No demo bundle is loaded yet. "
            "Run scripts/run_public_safety_demo_shell.py first.</p>"
        )

    context = {
        "demo_run": demo_run,
        "scenarios": list(demo_run.scenarios.all()),
        "top_activity_rows": list(demo_run.golden_activity_rows.all()[:8]),
        "recent_incident_rows": list(demo_run.incident_identity_rows.all()[:16]),
        "artifacts": _artifact_relative_paths(demo_run),
    }
    return render(request, "demo_shell/index.html", context)


def scenario_detail(request: HttpRequest, scenario_id: str) -> HttpResponse:
    demo_run = _latest_demo_run()
    if demo_run is None:
        raise Http404("No demo run loaded")

    scenario = get_object_or_404(DemoScenario, demo_run=demo_run, scenario_id=scenario_id)
    activity_row = (
        GoldenPersonActivity.objects.filter(demo_run=demo_run, golden_id=scenario.golden_id)
        .order_by("-total_incident_count", "golden_id")
        .first()
    )
    incident_rows = list(
        IncidentIdentity.objects.filter(demo_run=demo_run, golden_id=scenario.golden_id).order_by(
            "-occurred_at", "incident_id", "id"
        )[:20]
    )
    context = {
        "demo_run": demo_run,
        "scenario": scenario,
        "activity_row": activity_row,
        "incident_rows": incident_rows,
        "artifacts": _artifact_relative_paths(demo_run),
    }
    return render(request, "demo_shell/scenario_detail.html", context)


def golden_detail(request: HttpRequest, golden_id: str) -> HttpResponse:
    demo_run = _latest_demo_run()
    if demo_run is None:
        raise Http404("No demo run loaded")

    activity_row = get_object_or_404(
        GoldenPersonActivity,
        demo_run=demo_run,
        golden_id=golden_id,
    )
    incident_rows = list(
        IncidentIdentity.objects.filter(demo_run=demo_run, golden_id=golden_id).order_by(
            "-occurred_at", "incident_id", "id"
        )[:25]
    )
    context = {
        "demo_run": demo_run,
        "activity_row": activity_row,
        "incident_rows": incident_rows,
        "artifacts": _artifact_relative_paths(demo_run),
    }
    return render(request, "demo_shell/golden_detail.html", context)


def artifact_file(request: HttpRequest, relative_path: str) -> FileResponse:
    bundle_root = Path(settings.PUBLIC_SAFETY_DEMO_BUNDLE_ROOT).resolve()
    requested = (bundle_root / relative_path).resolve()
    if requested != bundle_root and bundle_root not in requested.parents:
        raise Http404("Artifact not found")
    if not requested.exists() or not requested.is_file():
        raise Http404("Artifact not found")

    content_type, _ = mimetypes.guess_type(str(requested))
    return FileResponse(requested.open("rb"), content_type=content_type or "application/octet-stream")
